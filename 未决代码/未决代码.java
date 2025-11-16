package com.jdyx.cx.measure.service.impl;


import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.core.toolkit.Wrappers;
import com.jdyx.common.enums.EvaluateMethodTypeEnum;
import com.jdyx.cx.measure.service.MeasureCxUnsettledService;
import com.jdyx.measure.api.measure.domain.*;
import com.jdyx.measure.api.measure.mapper.*;
import com.kevin.common.core.domain.R;
import com.kevin.common.utils.DateUtils;
import com.kevin.common.utils.StringUtils;
import com.kevin.common.utils.uuid.IdUtils;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import javax.annotation.Resource;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.Period;
import java.time.format.DateTimeParseException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.stream.Collectors;
import java.util.stream.IntStream;


/**
 * 未决计量服务实现类
 * @author 陈佳能
 * 日期：2025/8/1 17:50
 */
@Slf4j
@RequiredArgsConstructor
@Service
public class MeasureCxUnsettledServiceImpl implements MeasureCxUnsettledService {
  @Resource
  private ConfMeasureActuarialAssumptionMapper confMeasureActuarialAssumptionMapper;
  @Resource
  private ConfMeasureClaimModelNewMapper measureClaimModelNewMapper;
  @Resource
  private MeasureCxUnsettledMapper measureCxUnsettledMapper;
  @Resource
  private ConfMeasureMonthDisrateMapper measureMonthDisrateMapper;
  @Resource
  private  IntTPpJlUnsettledGroupMapper tPpJlUnsettledGroupMapper;

  @Autowired
  private JdbcTemplate jdbcTemplate;

  // 常量定义
  private static final int BATCH_SIZE = 100000;
  // 定义一个静态的格式化器，避免重复创建，线程安全
  //赔付模式进展因子缓存对象
  private final Map<String, BigDecimal[]> discountFactorCache = new ConcurrentHashMap<>();
  //精算假设缓存对象
  private final Map<String, Map<String, ConfMeasureActuarialAssumption>> assumptionCache = new ConcurrentHashMap<>();
  //月度远期利率缓存对象
  private final Map<String, Map<Integer, BigDecimal>> disRateCache = new ConcurrentHashMap<>();
  //上期未决结果
  private final Map<String, MeasureCxUnsettled> lastUnsettledMap = new ConcurrentHashMap<>();

//  @Resource(name = "threadPoolExecutor")
//  private ThreadPoolExecutor threadPoolExecutor;

  @Override
  public R<?> getUnsettledMeasureResult(String valMethod, String valMonth) {
    try {
      log.info("开始未决计量计算，评估方法: {}, 评估月份: {}", valMethod, valMonth);
      long startTime = System.currentTimeMillis();
      // 解决历史数据污染的问题
      clearCache();
      // 1. 预加载缓存数据
      preloadCacheData(valMethod,valMonth);
      log.info("缓存预加载完成，耗时: {} 秒", (System.currentTimeMillis() - startTime) / 1000);

      // 2. 使用游标分页+并行处理
      processDataWithCursorPagination(valMethod, valMonth);

      // 3. 汇总结果
      log.info("未决计量计算完成，总耗时: {} 秒", (System.currentTimeMillis() - startTime) / 1000);
      return R.ok();
    } catch (Exception e) {
      log.error("未决计量计算失败", e);
      return R.fail("未决计量计算失败: " + e.getMessage());
    } finally {
      // 清理缓存
      clearCache();
    }
  }

  /**
   * 预加载缓存数据
   */
  private void preloadCacheData(String valMethod,String valMonth) {
    // 1. 预加载精算假设数据到缓存
    LambdaQueryWrapper<ConfMeasureActuarialAssumption> assumptionQuery = Wrappers.lambdaQuery();
    assumptionQuery.eq(ConfMeasureActuarialAssumption::getValMethod, valMethod);
    List<ConfMeasureActuarialAssumption> assumptions = confMeasureActuarialAssumptionMapper.selectList(assumptionQuery);
    Map<String, ConcurrentHashMap<String, ConfMeasureActuarialAssumption>> collect =
      assumptions.stream()
        .collect(Collectors.groupingBy(
          ConfMeasureActuarialAssumption::getValMonth,  // 外层 key: valMonth
          Collectors.toMap(
            ConfMeasureActuarialAssumption::getClassCode,  // 内层 key: classCode
            assumption -> assumption,  // value: 对象本身
            (existing, replacement) -> existing,  // 冲突时保留现有值
            ConcurrentHashMap::new  // 内层 Map 使用 ConcurrentHashMap
          )
        ));

    // 2. 预加载赔付模式数据到缓存
    LambdaQueryWrapper<ConfMeasureClaimModelNew> claimModelQuery = Wrappers.lambdaQuery();
    claimModelQuery.orderByAsc(ConfMeasureClaimModelNew::getMonthId);
    List<ConfMeasureClaimModelNew> claimModels = measureClaimModelNewMapper.selectList(claimModelQuery);
    // 按 classCode 分组，并提取每个 classCode 对应的 paidRatio 数组
    Map<String, BigDecimal[]> claimModelMap = claimModels.stream()
      .collect(Collectors.groupingBy(
        ConfMeasureClaimModelNew::getClassCode,
        Collectors.mapping(
          ConfMeasureClaimModelNew::getPaidRatio,
          Collectors.collectingAndThen(
            Collectors.toList(),
            list -> list.toArray(new BigDecimal[0])
          )
        )
      ));

    //3.预加载月度远期利率
    LambdaQueryWrapper<ConfMeasureMonthDisrate> disrateQuery = Wrappers.lambdaQuery();
    disrateQuery.orderByAsc(ConfMeasureMonthDisrate::getTermMonth);
    List<ConfMeasureMonthDisrate> disrates = measureMonthDisrateMapper.selectList(disrateQuery);
    Map<String, Map<Integer, BigDecimal>> disrateMap = disrates.stream()
      .collect(Collectors.groupingBy(ConfMeasureMonthDisrate::getValMonth,
        Collectors.toMap(ConfMeasureMonthDisrate::getTermMonth, ConfMeasureMonthDisrate::getForwardDisrateValue)));

    //4.缓存上个评估期的未决结果
    LambdaQueryWrapper<MeasureCxUnsettled> lqw = new LambdaQueryWrapper<>();
    lqw.select(MeasureCxUnsettled::getAccidentMonth,MeasureCxUnsettled::getClassCode, MeasureCxUnsettled::getValMonth,MeasureCxUnsettled::getRa,
        MeasureCxUnsettled::getPvCaseAccident, MeasureCxUnsettled::getPvIbnrAccident,MeasureCxUnsettled::getPvUlaeAccident,
        MeasureCxUnsettled::getPvCaseCurrent,MeasureCxUnsettled::getPvIbnrCurrent, MeasureCxUnsettled::getPvUlaeCurrent,
        MeasureCxUnsettled::getUaleAmtIfieAccident,MeasureCxUnsettled::getIbnrAmtIfieAccident,MeasureCxUnsettled::getCaseAmtIfieAccident,
        MeasureCxUnsettled::getUnitId,MeasureCxUnsettled::getGroupId, MeasureCxUnsettled::getRiskCode,MeasureCxUnsettled::getComCode,
        MeasureCxUnsettled::getBusinessNature,MeasureCxUnsettled::getCarKindCode, MeasureCxUnsettled::getUseNatureCode,
        MeasureCxUnsettled::getReinType,MeasureCxUnsettled::getReinSystemCode)
      .eq(MeasureCxUnsettled::getValMonth, DateUtils.lastEndMonth(valMonth))
      .eq(MeasureCxUnsettled::getValMethod, valMethod)
      .eq(MeasureCxUnsettled::getDecidedFlag, "0");//只查找没有转已决的上期数据
    List<MeasureCxUnsettled> measureCxUnsettledLastList = measureCxUnsettledMapper.selectList(lqw);
    Map<String, MeasureCxUnsettled> unsettledMap = Optional.ofNullable(measureCxUnsettledLastList)
      .map(list -> {
        return list.stream()
          .collect(Collectors.toMap(
            entity -> StringUtils.joinWith("_",entity.getValMonth(),entity.getAccidentMonth(),entity.getUnitId()),
            entity -> entity, //
            (existingValue, newValue) -> existingValue // 冲突时保留第一个
          ));
      })
      .orElse(new HashMap<>());

    //放入缓存
    assumptionCache.putAll(collect);
    discountFactorCache.putAll(claimModelMap);
    disRateCache.putAll(disrateMap);
    lastUnsettledMap.putAll(unsettledMap);

    log.info("预加载完成 - 精算假设: {} 条, 赔付模式: {} 个险类,月度远期利率:{}条,上期未决结果:{}条",
      assumptions.size(), claimModelMap.size(),disrateMap.size(),unsettledMap.size());
  }

  /**
   * 使用游标分页+并行处理数据
   */
  private void processDataWithCursorPagination(String valMethod, String valMonth) {
    //清空当期数据
    measureCxUnsettledMapper.delete(new LambdaQueryWrapper<MeasureCxUnsettled>()
      .eq(MeasureCxUnsettled::getValMonth, valMonth)
      .eq(MeasureCxUnsettled::getValMethod, valMethod));

    long maxId = 0; // 游标
    int x = 1;

    while (true) {
      Long startTime = System.currentTimeMillis();
      // 使用游标方式分页查询
      LambdaQueryWrapper<IntTPpJlUnsettledGroup> lqw = new LambdaQueryWrapper<>();
      lqw.select(IntTPpJlUnsettledGroup::getId, IntTPpJlUnsettledGroup::getAccidentMonth,IntTPpJlUnsettledGroup::getClassCode,
          IntTPpJlUnsettledGroup::getCaseAmt,IntTPpJlUnsettledGroup::getIbnrAmt,IntTPpJlUnsettledGroup::getUlaeAmt,IntTPpJlUnsettledGroup::getUnitId,
          IntTPpJlUnsettledGroup::getRiskCode,IntTPpJlUnsettledGroup::getComCode,IntTPpJlUnsettledGroup::getBusinessNature,IntTPpJlUnsettledGroup::getCarKindCode,
          IntTPpJlUnsettledGroup::getUseNatureCode,IntTPpJlUnsettledGroup::getGroupId,IntTPpJlUnsettledGroup::getReinType,IntTPpJlUnsettledGroup::getReinSystemCode)
        .eq(IntTPpJlUnsettledGroup::getValMonth, valMonth)
        .eq(IntTPpJlUnsettledGroup::getValMethod, valMethod)
        .gt(IntTPpJlUnsettledGroup::getId, maxId)
        .orderByAsc(IntTPpJlUnsettledGroup::getId)
        .last("LIMIT " + BATCH_SIZE);
      List<IntTPpJlUnsettledGroup> records = tPpJlUnsettledGroupMapper.selectList(lqw);
      if (records.isEmpty()) {
        break;
      }
      log.debug("页数:{},耗时: {}ms", x++, System.currentTimeMillis() - startTime);
      // 处理批次数据
      processBatchAsync(records, valMonth,valMethod);
      // 更新游标
      maxId = records.get(records.size() - 1).getId();
    }
    //处理上期未决转已决的数据
    dealDecidedData(valMonth,valMethod);
  }

  /**
   * 异步处理批次数据
   */
  private void processBatchAsync(List<IntTPpJlUnsettledGroup> batchData, String valMonth,String valMethod) {

    // 将参数声明为final，避免lambda表达式中的变量引用问题
    final String finalValMonth = valMonth;
    final String finalValMethod = valMethod;
    final List<IntTPpJlUnsettledGroup> finalBatchData = batchData;

//    threadPoolExecutor.execute(() -> {
      try {
        long startTime = System.currentTimeMillis();
        //并行流处理代替向量化计算
        List<MeasureCxUnsettled> batchResults = finalBatchData.stream()
          .map(contract -> calculateLrcWithMonthlyRolling(contract, finalValMonth,finalValMethod))
          .collect(Collectors.toList());
        //批量插入数据库
//        measureCxUnsettledMapper.insertBatch(batchResults);
        insertBatchWithJdbcTemplate(batchResults);
        log.debug("批次处理完成，数据量: {}, 耗时: {} ms",
          finalBatchData.size(), System.currentTimeMillis() - startTime);
      } catch (Exception e) {
        log.error("批次处理异常", e);
      }
//      } finally {
////        latch.countDown();
//      }
//    });
  }

  private void insertBatchWithJdbcTemplate(List<MeasureCxUnsettled> allResults) {
    if (CollectionUtils.isEmpty(allResults)) {
      return;
    }
    // 1. 定义所有要插入的字段名
    List<String> columnNames = Arrays.asList(
      "id", "val_month", "val_method", "risk_code", "group_id", "case_amt", "ibnr_amt", "ulae_amt",
      "pv_case_current", "pv_ibnr_current", "pv_ulae_current", "pv_case_accident", "pv_ibnr_accident",
      "pv_ulae_accident", "paid_claim_change", "service_fee_change", "paid_claim_ifie", "oci_change",
      "pv_last_ulae_accident", "pv_last_ibnr_accident", "pv_last_case_accident", "pv_last_ulae_current",
      "pv_last_ibnr_current", "pv_last_case_current", "pv_last_ulae_amt", "pv_last_ibnr_amt",
      "pv_last_case_amt", "uale_amt_ifie_accident", "ibnr_amt_ifie_accident", "case_amt_ifie_accident",
      "accident_month", "decided_flag", "current_flag", "class_code", "com_code", "business_nature",
      "car_kind_code", "use_nature_code", "ra", "unit_id", "create_by", "update_by", "update_time",
      "create_time", "rein_type", "rein_system_code"
    );

    // 2. 动态构建 SQL
    String columnsPart = columnNames.stream()
      .map(name -> "\"" + name + "\"")
      .collect(Collectors.joining(", "));

    String placeholdersPart = columnNames.stream()
      .map(name -> "?")
      .collect(Collectors.joining(", "));

    String sql = String.format("INSERT INTO measure_platform.measure_cx_unsettled (%s) VALUES (%s)", columnsPart, placeholdersPart);

    jdbcTemplate.batchUpdate(sql, new BatchPreparedStatementSetter() {
      @Override
      public void setValues(PreparedStatement ps, int i) throws SQLException {
        MeasureCxUnsettled item = allResults.get(i);
        int index = 1;

        // 严格按照 columnNames 的顺序设置参数
        ps.setLong(index++, item.getId());
        ps.setString(index++, item.getValMonth());
        ps.setString(index++, item.getValMethod());
        ps.setString(index++, item.getRiskCode());
        ps.setString(index++, item.getGroupId());
        ps.setBigDecimal(index++, item.getCaseAmt());
        ps.setBigDecimal(index++, item.getIbnrAmt());
        ps.setBigDecimal(index++, item.getUlaeAmt());
        ps.setBigDecimal(index++, item.getPvCaseCurrent());
        ps.setBigDecimal(index++, item.getPvIbnrCurrent());
        ps.setBigDecimal(index++, item.getPvUlaeCurrent());
        ps.setBigDecimal(index++, item.getPvCaseAccident());
        ps.setBigDecimal(index++, item.getPvIbnrAccident());
        ps.setBigDecimal(index++, item.getPvUlaeAccident());
        ps.setBigDecimal(index++, item.getPaidClaimChange());
        ps.setBigDecimal(index++, item.getServiceFeeChange());
        ps.setBigDecimal(index++, item.getPaidClaimIfie());
        ps.setBigDecimal(index++, item.getOciChange());
        ps.setBigDecimal(index++, item.getPvLastUlaeAccident());
        ps.setBigDecimal(index++, item.getPvLastIbnrAccident());
        ps.setBigDecimal(index++, item.getPvLastCaseAccident());
        ps.setBigDecimal(index++, item.getPvLastUlaeCurrent());
        ps.setBigDecimal(index++, item.getPvLastIbnrCurrent());
        ps.setBigDecimal(index++, item.getPvLastCaseCurrent());
        ps.setBigDecimal(index++, item.getPvLastUlaeAmt());
        ps.setBigDecimal(index++, item.getPvLastIbnrAmt());
        ps.setBigDecimal(index++, item.getPvLastCaseAmt());
        ps.setBigDecimal(index++, item.getUaleAmtIfieAccident());
        ps.setBigDecimal(index++, item.getIbnrAmtIfieAccident());
        ps.setBigDecimal(index++, item.getCaseAmtIfieAccident());
        ps.setString(index++, item.getAccidentMonth());
        ps.setString(index++, item.getDecidedFlag());
        ps.setString(index++, item.getCurrentFlag());
        ps.setString(index++, item.getClassCode());
        ps.setString(index++, item.getComCode());
        ps.setString(index++, item.getBusinessNature());
        ps.setString(index++, item.getCarKindCode());
        ps.setString(index++, item.getUseNatureCode());
        ps.setBigDecimal(index++, item.getRa());
        ps.setString(index++, item.getUnitId());
        ps.setString(index++, item.getCreateBy());
        ps.setString(index++, item.getUpdateBy());
        ps.setTimestamp(index++, new Timestamp(System.currentTimeMillis()));
        ps.setTimestamp(index++, new Timestamp(System.currentTimeMillis()));
        ps.setString(index++, item.getReinType());
        ps.setString(index++, item.getReinSystemCode());
      }

      @Override
      public int getBatchSize() {
        return allResults.size();
      }
    });
  }

  /**
   * 过渡期从保险责任起期滚动计量到评估时点
   */
  private MeasureCxUnsettled calculateLrcWithMonthlyRolling(IntTPpJlUnsettledGroup contract, String valMonth ,String valMethod) {
    //获取评估大类对应的精算假设
    Map<String, ConfMeasureActuarialAssumption> actuarialAssumptionMap = assumptionCache.get(valMonth);
    ConfMeasureActuarialAssumption assumption = actuarialAssumptionMap.getOrDefault(contract.getClassCode(),new ConfMeasureActuarialAssumption());
    BigDecimal ra = assumption.getLicRa();
    //上期未决结果
    MeasureCxUnsettled lastUnsettled = lastUnsettledMap.getOrDefault(StringUtils.joinWith("_",DateUtils.lastEndMonth(valMonth),contract.getAccidentMonth(), contract.getUnitId()), new MeasureCxUnsettled());
    //出险年月的月度远期利率曲线
    Map<Integer, BigDecimal> accidentRateMap = disRateCache.getOrDefault(contract.getAccidentMonth(),new HashMap<>());
    //评估时点的月度远期利率曲线
    Map<Integer, BigDecimal> valMonthsRateMap = disRateCache.getOrDefault(valMonth,new HashMap<>());
    //当期评估大类对应的未决赔付模式进展因子
    BigDecimal[] claimFactorArr = discountFactorCache.get(contract.getClassCode());
    if(claimFactorArr == null){
      throw new RuntimeException("未配置险类对应的未决赔付模式进展因子，险类代码:"+contract.getClassCode());
    }
    //当前评估月与出险月的月数差
    int n = monthsBetween(contract.getAccidentMonth(), valMonth);
    // 1. 本评估时点未决赔款现金流的本期末现值（用当期利率曲线折现）
    BigDecimal pvUlaeCurrent = getPvLoss(contract.getUlaeAmt(), claimFactorArr, valMonthsRateMap,n,Boolean.TRUE);
    BigDecimal pvIbnrCurrent = getPvLoss(contract.getIbnrAmt(), claimFactorArr, valMonthsRateMap,n,Boolean.TRUE);
    BigDecimal pvCaseCurrent = getPvLoss(contract.getCaseAmt(), claimFactorArr, valMonthsRateMap,n,Boolean.TRUE);
    //2.上一评估时点未决赔付现金流的上一评估时点现值（用上期利率曲线折现）
    BigDecimal pvLastUlaeCurrentAmt = lastUnsettled.getPvUlaeCurrent() == null ? BigDecimal.ZERO : lastUnsettled.getPvUlaeCurrent();
    BigDecimal pvLastIbnrCurrentAmt = lastUnsettled.getPvIbnrCurrent() == null ? BigDecimal.ZERO : lastUnsettled.getPvIbnrCurrent();
    BigDecimal pvLastCaseCurrentAmt = lastUnsettled.getPvCaseCurrent() == null ? BigDecimal.ZERO : lastUnsettled.getPvCaseCurrent();
    //3.本评估时点未决赔付现金流的本期末现值（用事故发生时的锁定利率曲线折现）
    BigDecimal pvUlaeAccident = getPvLoss(contract.getUlaeAmt(), claimFactorArr, accidentRateMap,n,Boolean.FALSE);
    BigDecimal pvIbnrAccident = getPvLoss(contract.getIbnrAmt(), claimFactorArr, accidentRateMap,n,Boolean.FALSE);
    BigDecimal pvCaseAccident = getPvLoss(contract.getCaseAmt(), claimFactorArr, accidentRateMap,n,Boolean.FALSE);
    //4.上一评估时点未决赔款现金流的上一评估时点现值（用事故发生时的锁定利率曲线折现）
    BigDecimal pvLastUlaeAccidentAmt = lastUnsettled.getPvUlaeAccident() == null ? BigDecimal.ZERO : lastUnsettled.getPvUlaeAccident();
    BigDecimal pvLastIbnrAccidentAmt = lastUnsettled.getPvIbnrAccident() == null ? BigDecimal.ZERO : lastUnsettled.getPvIbnrAccident();
    BigDecimal pvLastCaseAccidentAmt = lastUnsettled.getPvCaseAccident() == null ? BigDecimal.ZERO : lastUnsettled.getPvCaseAccident();
    //本评估时点未决赔款现金流的下期末现值（用事故发生时的锁定利率曲线计息）
    BigDecimal ualeAmtIfieAccident = pvUlaeAccident.multiply(accidentRateMap.get(n).add(BigDecimal.ONE)).setScale(10, RoundingMode.HALF_UP);
    BigDecimal ibnrAmtIfieAccident = pvIbnrAccident.multiply(accidentRateMap.get(n).add(BigDecimal.ONE)).setScale(10, RoundingMode.HALF_UP);
    BigDecimal caseAmtIfieAccident = pvCaseAccident.multiply(accidentRateMap.get(n).add(BigDecimal.ONE)).setScale(10, RoundingMode.HALF_UP);
    //5.上一评估时点未决赔款现金流的本期末现值（用事故发生时的锁定利率曲线计息）
    BigDecimal pvLastUlaeAmt = lastUnsettled.getUaleAmtIfieAccident() == null ? BigDecimal.ZERO : lastUnsettled.getUaleAmtIfieAccident();
    BigDecimal pvLastIbnrAmt = lastUnsettled.getIbnrAmtIfieAccident() == null ? BigDecimal.ZERO : lastUnsettled.getIbnrAmtIfieAccident();
    BigDecimal pvLastCaseAmt = lastUnsettled.getCaseAmtIfieAccident() == null ? BigDecimal.ZERO : lastUnsettled.getCaseAmtIfieAccident();

    //已发生赔款负债变动变动:1-2
    BigDecimal currentAmt = pvUlaeCurrent.add(pvIbnrCurrent).add(pvCaseCurrent);
    BigDecimal lastCurrentAmt = pvLastUlaeCurrentAmt.add(pvLastIbnrCurrentAmt).add(pvLastCaseCurrentAmt);
    BigDecimal paidClaimChange = currentAmt.subtract(lastCurrentAmt).setScale(10, RoundingMode.HALF_UP);

    //未来履约现金流变动计入保险服务费用:3-5
    BigDecimal currentAccident = pvUlaeAccident.add(pvIbnrAccident).add(pvCaseAccident);
    BigDecimal lastAccidentIfie = pvLastUlaeAmt.add(pvLastIbnrAmt).add(pvLastCaseAmt);
    BigDecimal serviceFeeChange = currentAccident.subtract(lastAccidentIfie).setScale(10, RoundingMode.HALF_UP);

    //已发生赔款负债计提利息计入保险财务费用:5-4
    BigDecimal lastAccidentAmt = pvLastUlaeAccidentAmt.add(pvLastIbnrAccidentAmt).add(pvLastCaseAccidentAmt);
    BigDecimal paidClaimIfie = lastAccidentIfie.subtract(lastAccidentAmt).setScale(10, RoundingMode.HALF_UP);

    //折现率变动可选择计入OCI:1-2-3+4
    BigDecimal ociChange = paidClaimChange.subtract(currentAccident).add(lastAccidentAmt).setScale(10, RoundingMode.HALF_UP);

    //移除上期已经匹配到到数据
    if(!lastUnsettledMap.isEmpty()){
      lastUnsettledMap.remove(StringUtils.joinWith("_",DateUtils.lastEndMonth(valMonth),contract.getAccidentMonth(),contract.getUnitId()));
    }

    //保存结果
    MeasureCxUnsettled measureCxUnsettled = new MeasureCxUnsettled();
    measureCxUnsettled.setId(contract.getId());
    measureCxUnsettled.setValMonth(valMonth);
    measureCxUnsettled.setValMethod(valMethod);
    measureCxUnsettled.setClassCode(contract.getClassCode());
    measureCxUnsettled.setAccidentMonth(contract.getAccidentMonth());
    measureCxUnsettled.setRa(ra);
    measureCxUnsettled.setComCode(contract.getComCode());
    measureCxUnsettled.setBusinessNature(contract.getBusinessNature());
    measureCxUnsettled.setCarKindCode(contract.getCarKindCode());
    measureCxUnsettled.setUseNatureCode(contract.getUseNatureCode());
    measureCxUnsettled.setRiskCode(contract.getRiskCode());
    measureCxUnsettled.setGroupId(contract.getGroupId());
    measureCxUnsettled.setUnitId(contract.getUnitId());
    measureCxUnsettled.setReinType(contract.getReinType());
    measureCxUnsettled.setReinSystemCode(contract.getReinSystemCode());
    //当期未决赔款
    measureCxUnsettled.setUlaeAmt(contract.getUlaeAmt());
    measureCxUnsettled.setIbnrAmt(contract.getIbnrAmt());
    measureCxUnsettled.setCaseAmt(contract.getCaseAmt());
    //本评估时点未决赔款现金流的本期末现值（用事故时点利率曲线折现）
    measureCxUnsettled.setPvUlaeAccident(pvUlaeAccident);
    measureCxUnsettled.setPvIbnrAccident(pvIbnrAccident);
    measureCxUnsettled.setPvCaseAccident(pvCaseAccident);
    //本评估时点未决赔款现金流的本期末现值（用当期利率曲线折现）
    measureCxUnsettled.setPvUlaeCurrent(pvUlaeCurrent);
    measureCxUnsettled.setPvIbnrCurrent(pvIbnrCurrent);
    measureCxUnsettled.setPvCaseCurrent(pvCaseCurrent);
    //上一评估时点未决赔付现金流的上一评估时点现值（用上期利率曲线折现）
    measureCxUnsettled.setPvLastUlaeCurrent(pvLastUlaeCurrentAmt);
    measureCxUnsettled.setPvLastIbnrCurrent(pvLastIbnrCurrentAmt);
    measureCxUnsettled.setPvLastCaseCurrent(pvLastCaseCurrentAmt);
    //上一评估时点未决赔付现金流的上一评估时点现值（用事故时点利率曲线折现）
    measureCxUnsettled.setPvLastUlaeAccident(pvLastUlaeAccidentAmt);
    measureCxUnsettled.setPvLastIbnrAccident(pvLastIbnrAccidentAmt);
    measureCxUnsettled.setPvLastCaseAccident(pvLastCaseAccidentAmt);
    //上一评估时点未决赔款现金流的本期末现值（用事故发生时的锁定利率曲线计息）
    measureCxUnsettled.setPvLastUlaeAmt(pvLastUlaeAmt);
    measureCxUnsettled.setPvLastIbnrAmt(pvLastIbnrAmt);
    measureCxUnsettled.setPvLastCaseAmt(pvLastCaseAmt);
    //本评估时点未决赔款现金流的本期末现值（用事故发生时的锁定利率曲线计息）
    measureCxUnsettled.setUaleAmtIfieAccident(ualeAmtIfieAccident);
    measureCxUnsettled.setIbnrAmtIfieAccident(ibnrAmtIfieAccident);
    measureCxUnsettled.setCaseAmtIfieAccident(caseAmtIfieAccident);
    //分录结果
    measureCxUnsettled.setPaidClaimChange(paidClaimChange);
    measureCxUnsettled.setServiceFeeChange(serviceFeeChange);
    measureCxUnsettled.setPaidClaimIfie(paidClaimIfie);
    measureCxUnsettled.setOciChange(ociChange);
    //如果事故年等于评估年，则为当期，否则为往期
    String accidentYear = contract.getAccidentMonth().substring(0, 4);
    String coverYear = valMonth.substring(0, 4);
    measureCxUnsettled.setCurrentFlag(accidentYear.equals(coverYear) ? "1" : "0");
    measureCxUnsettled.setDecidedFlag("0");


    return measureCxUnsettled;
  }

  /**
   *根据赔付模式进度因子数组计算在折现
   *例如赔付模式进度因子数组为[0.05, 0.95]，金额是100
   *对result[5,95]折现到当前评估时点的现值
   *
   * @param lossAmt 赔付费用
   * @param claimFactorArr 赔付模型进展因子
   * @param monthsRateMap 月度远期利率曲线
   * @param n 月度远期利率期间与出险月的月数差
   * @return
   */
  private BigDecimal getPvLoss(BigDecimal lossAmt,BigDecimal[] claimFactorArr,Map<Integer, BigDecimal> monthsRateMap,int n,Boolean isCurrent) {
    if (lossAmt.compareTo(BigDecimal.ZERO) == 0) {
      return BigDecimal.ZERO;
    }
    // 1. 获取剩余进展因子数组
    BigDecimal[] remainingFactors = n <= claimFactorArr.length - 1 ?
      Arrays.copyOfRange(claimFactorArr, n, claimFactorArr.length) :
      new BigDecimal[]{BigDecimal.ONE};

    // 2. 计算剩余进展因子总和
    BigDecimal sum = Arrays.stream(remainingFactors).reduce(BigDecimal.ZERO, BigDecimal::add);
    if (sum.compareTo(BigDecimal.ZERO) == 0) {
      return BigDecimal.ZERO; // 避免除以零
    }

    // 3. 使用流式计算，避免可变状态
    final int startPeriod = isCurrent ? 1 : n;

    return IntStream.range(0, remainingFactors.length)
      .mapToObj(i -> {
        // 计算第i期的赔付金额
        BigDecimal cashFlow = lossAmt.multiply(remainingFactors[i]).divide(sum, 10, RoundingMode.HALF_UP);

        // 计算第i期的折现因子
        BigDecimal discountFactor = BigDecimal.ONE;
        for (int j = 0; j <= i; j++) {
          BigDecimal rate = monthsRateMap.get(startPeriod + j);
          if (rate == null) {
            // 处理利率缺失的情况，抛出异常或使用默认值
            throw new RuntimeException(String.format("月度远期利率缺失, 期间: %d", startPeriod + j));
          }
          discountFactor = discountFactor.multiply(rate.add(BigDecimal.ONE)).setScale(10, RoundingMode.HALF_UP);
        }

        // 返回第i期赔付费用的现值
        return cashFlow.divide(discountFactor, 10, RoundingMode.HALF_UP);
      })
      .reduce(BigDecimal.ZERO, BigDecimal::add); // 将所有现值相加
  }
  public int monthsBetween(String valMonth1, String valMonth2) {
    try {
      // 1. 解析字符串为 YearMonth 对象
      LocalDate startYearMonth = LocalDate.parse(DateUtils.parseDateToStr(DateUtils.YYYY_MM_DD, DateUtils.parseDate(valMonth1)));
      LocalDate endYearMonth = LocalDate.parse(DateUtils.parseDateToStr(DateUtils.YYYY_MM_DD, DateUtils.parseDate(valMonth2)));

      // 2. 使用 Period 计算间隔
      // 注意：Period.between() 的计算结果是 end - start
      Period period = Period.between(startYearMonth, endYearMonth);

      // 3. 计算总月数
      // period.getYears() 获取年份差，period.getMonths() 获取月份差
      return period.getYears() * 12 + period.getMonths() + 1;

    } catch (DateTimeParseException e) {
      throw new IllegalArgumentException("无法解析的日期字符串，请确保是有效的年月，例如：202308", e);
    }
  }

  /**
   * 清理缓存
   */
  private void clearCache() {
    discountFactorCache.clear();
    assumptionCache.clear();
    disRateCache.clear();
    lastUnsettledMap.clear();
    log.info("缓存清理完成");
  }

  /**
   * 处理上期未决转已决的数据
   * @param valMonth 评估月
   * @param valMethod 评估方法
   */
  void dealDecidedData(String valMonth,String valMethod){
    List<MeasureCxUnsettled> measureCxUnsettledList = new ArrayList<>();
    for(Map.Entry<String,MeasureCxUnsettled> entry : lastUnsettledMap.entrySet()){
      //保存结果
      MeasureCxUnsettled measureCxUnsettled = entry.getValue();
      measureCxUnsettled.setId(IdUtils.getSnowFlakeLongId());
      measureCxUnsettled.setValMonth(valMonth);
      measureCxUnsettled.setValMethod(valMethod);
      //2. 上一评估时点未决赔付现金流的上一评估时点现值（用上期利率曲线折现）
      measureCxUnsettled.setPvLastUlaeCurrent(measureCxUnsettled.getPvUlaeCurrent());
      measureCxUnsettled.setPvLastIbnrCurrent(measureCxUnsettled.getPvIbnrCurrent());
      measureCxUnsettled.setPvLastCaseCurrent(measureCxUnsettled.getPvCaseCurrent());
      //4. 上一评估时点未决赔付现金流的上一评估时点现值（用事故时点利率曲线折现）
      measureCxUnsettled.setPvLastUlaeAccident(measureCxUnsettled.getPvUlaeAccident());
      measureCxUnsettled.setPvLastIbnrAccident(measureCxUnsettled.getPvIbnrAccident());
      measureCxUnsettled.setPvLastCaseAccident(measureCxUnsettled.getPvCaseAccident());
      //5. 上一评估时点未决赔款现金流的本期末现值（用事故发生时的锁定利率曲线计息）
      measureCxUnsettled.setPvLastUlaeAmt(measureCxUnsettled.getUaleAmtIfieAccident());
      measureCxUnsettled.setPvLastIbnrAmt(measureCxUnsettled.getIbnrAmtIfieAccident());
      measureCxUnsettled.setPvLastCaseAmt(measureCxUnsettled.getCaseAmtIfieAccident());
      //1. 本评估时点未决赔款现金流的本期末现值（用当期利率曲线折现）
      measureCxUnsettled.setPvUlaeAccident(BigDecimal.ZERO);
      measureCxUnsettled.setPvIbnrAccident(BigDecimal.ZERO);
      measureCxUnsettled.setPvCaseAccident(BigDecimal.ZERO);
      //3. 本评估时点未决赔付现金流的本期末现值（用事故发生时的锁定利率曲线折现）
      measureCxUnsettled.setPvUlaeCurrent(BigDecimal.ZERO);
      measureCxUnsettled.setPvIbnrCurrent(BigDecimal.ZERO);
      measureCxUnsettled.setPvCaseCurrent(BigDecimal.ZERO);

      measureCxUnsettled.setUlaeAmt(BigDecimal.ZERO);
      measureCxUnsettled.setIbnrAmt(BigDecimal.ZERO);
      measureCxUnsettled.setCaseAmt(BigDecimal.ZERO);

      measureCxUnsettled.setUaleAmtIfieAccident(BigDecimal.ZERO);
      measureCxUnsettled.setIbnrAmtIfieAccident(BigDecimal.ZERO);
      measureCxUnsettled.setCaseAmtIfieAccident(BigDecimal.ZERO);
      //-2
      BigDecimal paidClaimChange = measureCxUnsettled.getPvLastUlaeCurrent()
        .add(measureCxUnsettled.getPvLastIbnrCurrent())
        .add(measureCxUnsettled.getPvLastCaseCurrent())
        .setScale(10, RoundingMode.HALF_UP)
        .negate();
      //-5
      BigDecimal serviceFeeChange = measureCxUnsettled.getPvLastUlaeAmt()
        .add(measureCxUnsettled.getPvLastIbnrAmt())
        .add(measureCxUnsettled.getPvLastCaseAmt())
        .setScale(10, RoundingMode.HALF_UP)
        .negate();
      //5-4
      BigDecimal pvlastAmt = measureCxUnsettled.getPvLastUlaeAmt().add(measureCxUnsettled.getPvLastIbnrAmt()).add(measureCxUnsettled.getPvLastCaseAmt());
      BigDecimal pvLastAccident = measureCxUnsettled.getPvLastUlaeAccident().add(measureCxUnsettled.getPvLastIbnrAccident()).add(measureCxUnsettled.getPvLastCaseAccident());
      BigDecimal paidClaimIfie = pvlastAmt.subtract(pvLastAccident).setScale(10, RoundingMode.HALF_UP);
      //4-2
      BigDecimal ociChange = pvLastAccident
        .subtract(measureCxUnsettled.getPvLastUlaeCurrent())
        .subtract(measureCxUnsettled.getPvLastIbnrCurrent())
        .subtract(measureCxUnsettled.getPvLastCaseCurrent())
        .setScale(10, RoundingMode.HALF_UP);

      measureCxUnsettled.setPaidClaimChange(paidClaimChange);
      measureCxUnsettled.setServiceFeeChange(serviceFeeChange);
      measureCxUnsettled.setPaidClaimIfie(paidClaimIfie);
      measureCxUnsettled.setOciChange(ociChange);
      //是否转已决
      measureCxUnsettled.setDecidedFlag("1");
      measureCxUnsettled.setCurrentFlag(measureCxUnsettled.getAccidentMonth().equals(valMonth) ? "1" : "0");
      measureCxUnsettledList.add(measureCxUnsettled);
    }
//    measureCxUnsettledMapper.insertBatch(measureCxUnsettledList);
    insertBatchWithJdbcTemplate(measureCxUnsettledList);
  }
}

