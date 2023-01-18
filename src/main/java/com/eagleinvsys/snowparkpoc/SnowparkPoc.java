package com.eagleinvsys.snowparkpoc;

import com.snowflake.snowpark_java.*;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Slf4j
public class SnowparkPoc
{

    public static void main(String[] args) {
        // Replace the <placeholders> below.
        Map<String, String> properties = new HashMap<>();
        properties.put("URL", " https://bnyeagle_dev.east-us-2.azure.snowflakecomputing.com:443");
        properties.put("USER", "READ_ALL_DA1_D34_C_EAGLE");
        properties.put("PASSWORD", ")de361CG#5");
        properties.put("ROLE", "READ_ALL_DA1_D34_C_EAGLE");
        properties.put("WAREHOUSE", "DEV_WAREHOUSE");
        properties.put("DB", "EAGLE_DEV_WAREHOUSE_DA1_D34_C_EAGLE");
        properties.put("SCHEMA", "DA1D34CEAGLE");

        // Create snowflake session
        Session session = Session.builder().configs(properties).create();

        ReversalAdjustment reversalAdjustment = executeReversalAdjQuery(session);
        GLSum glSum = executeGLSum(session);
        BigDecimal priorRevision = executePriorRevisionQuery(session);
        BookCostBase bookCostBase = executeBookCostBaseQuery(session);
        List<SummaryRowData> summaryRowDataList = executeSummaryRowCollection(session);

        // todo - create short term & long term summary records
        List<SummaryRecord> shortTermSummaryRecords = getShortTermSummaryRecords(reversalAdjustment, glSum, priorRevision, bookCostBase, summaryRowDataList);
        List<SummaryRecord> longTermSummaryRecords = getLongTermSummaryRecords(reversalAdjustment, glSum, bookCostBase, summaryRowDataList);
        List<FinalReport> finalSummaryReportData = generateFinalSummaryReport(shortTermSummaryRecords, longTermSummaryRecords);

        writeSummaryInFile(finalSummaryReportData);
        // close session
        session.close();
    }

    @SneakyThrows
    private static void writeSummaryInFile(List<FinalReport> finalReportList)
    {
        FileWriter fileWriter = new FileWriter("FinalReport.csv");
        BufferedWriter writer = new BufferedWriter(fileWriter);

        finalReportList.forEach(x -> {
            try
            {
                writer.write(x.getStringValue());
                writer.newLine();
            }
            catch (IOException e)
            {
                throw new RuntimeException(e);
            }
        });
        writer.flush();
        writer.close();
    }

    private static List<FinalReport> generateFinalSummaryReport(List<SummaryRecord> shortTermSummaryRecords, List<SummaryRecord> longTermSummaryRecords)
    {
        List<FinalReport> finalSummaryReportData = new ArrayList<>();

        for(int i = 0; i < shortTermSummaryRecords.size(); i++)
        {
            FinalReport.FinalReportBuilder builder = FinalReport.builder();
            String titleA = shortTermSummaryRecords.get(i).getSummaryTitle();
            BigDecimal sumValueA = null;
            if (titleA.equalsIgnoreCase("Wash Sale Roll Forward")
            || titleA.equalsIgnoreCase("Long Holdings Tax Cost")
            || titleA.equalsIgnoreCase("Current Period Redemption in Kind (RIK) Activity"))
            {
                sumValueA = null;
            }
            else if (!titleA.equalsIgnoreCase(longTermSummaryRecords.get(i).getSummaryTitle())
            || (titleA.equalsIgnoreCase("ST to LT Book Holding Period Reclass")
                || titleA.equalsIgnoreCase("ST to LT Book Holding Period Reclass - RIK")))
            {
                sumValueA = shortTermSummaryRecords.get(i).getValue();
            }

            String titleB = null;
            if (longTermSummaryRecords.get(i).getSummaryTitle().equals("DUMMY"))
            {
                titleB = null;
            }
            else if (!titleA.equalsIgnoreCase(longTermSummaryRecords.get(i).getSummaryTitle())
                || (titleA.equalsIgnoreCase("ST to LT Book Holding Period Reclass")
                || titleA.equalsIgnoreCase("ST to LT Book Holding Period Reclass - RIK")))
            {
                titleB = longTermSummaryRecords.get(i).getSummaryTitle();
            }

            BigDecimal sumValueB = null;
            if(titleB == null || titleB.equalsIgnoreCase("Net Holdings Tax Cost")
                || titleB.equalsIgnoreCase("Short Holdings Tax Proceeds"))
            {
                sumValueB = null;
            }
            else if ( ((!titleB.equalsIgnoreCase(titleA)
                || (titleB.equalsIgnoreCase("ST to LT Book Holding Period Reclass")
                || titleB.equalsIgnoreCase("ST to LT Book Holding Period Reclass - RIK"))))
            && !titleB.equalsIgnoreCase("DUMMY"))
            {
                sumValueB = longTermSummaryRecords.get(i).getValue();
            }

            finalSummaryReportData.add(FinalReport.builder()
                                                  .entityId("BA32")
                                                  .acctBasis("USTAX")
                                                  .reportType("POC WS Report")
                                                  .reportRunDate(LocalDate.now())
                                                  .acctCurrency("USD")
                                                  .titleA(titleA)
                                                  .titleB(titleB)
                                                  .sumValueA(sumValueA)
                                                  .sumValueB(sumValueB)
                                                  .build());
        }
        return finalSummaryReportData;
    }

    private static List<SummaryRecord> getLongTermSummaryRecords(ReversalAdjustment reversalAdjustment, GLSum glSum, BookCostBase bookCostBase, List<SummaryRowData> dfFifthQuery)
    {
        // todo - Long Term Summary Record
        List<SummaryRecord> longTermSummaryRecords = new ArrayList<>();

        //todo - Add Empty record
        longTermSummaryRecords.add(SummaryRecord.builder().summaryTitle(dfFifthQuery.get(0).getTitle2())
                                                 .value(null)
                                                 .build());

        longTermSummaryRecords.add(SummaryRecord.builder().summaryTitle("Long Term Capital Gain")
                                                 .value(dfFifthQuery.stream()
                                                                    .map(SummaryRowData::getTitle2LongTermGainBase)
                                                                    .reduce(BigDecimal.ZERO, BigDecimal::add))
                                                 .build());
        longTermSummaryRecords.add(SummaryRecord.builder().summaryTitle("Long Term Capital Loss")
                                                 .value(dfFifthQuery.stream()
                                                                    .map(SummaryRowData::getTitle2LongTermLossBase)
                                                                    .reduce(BigDecimal.ZERO, BigDecimal::add))
                                                 .build());

        longTermSummaryRecords.add(SummaryRecord.builder().summaryTitle("Net Long Term Capital Gain/Loss")
                                                 .value(dfFifthQuery.stream()
                                                                    .map(x -> (x.getTitle2LongTermGainBase().subtract(x.getTitle2LongTermLossBase())))
                                                                    .reduce(BigDecimal.ZERO, BigDecimal::add))
                                                 .build());

        longTermSummaryRecords.add(SummaryRecord.builder().summaryTitle("Long Term Sale Proceeds")
                                                 .value(dfFifthQuery.stream()
                                                                    .map(SummaryRowData::getTitle2LongTermSaleBase)
                                                                    .reduce(BigDecimal.ZERO, BigDecimal::add))
                                                 .build());
        longTermSummaryRecords.add(SummaryRecord.builder().summaryTitle("Long Term Book Cost")
                                                 .value(dfFifthQuery.stream()
                                                                    .map(SummaryRowData::getTitle2LongTermBookCost)
                                                                    .reduce(BigDecimal.ZERO, BigDecimal::add))
                                                 .build());

        longTermSummaryRecords.add(SummaryRecord.builder().summaryTitle(dfFifthQuery.get(0).getTitle3())
                                                 .value(null)
                                                 .build());

        longTermSummaryRecords.add(SummaryRecord.builder().summaryTitle("LT Capital Deferrals")
                                                 .value(BigDecimal.ZERO) // todo - not calculated , v_ws_l_def_b_m
                                                 .build());

        longTermSummaryRecords.add(SummaryRecord.builder().summaryTitle("LT Capital Reversals")
                                                 .value(BigDecimal.valueOf(-1).multiply(dfFifthQuery.stream()
                                                                                                    .map(SummaryRowData::getTitle3CurrentPeriodLTReversalBase)
                                                                                                    .reduce(BigDecimal.ZERO, BigDecimal::add)
                                                                                                    .add(reversalAdjustment.getLongTermReversalAdjustment())))
                                                 .build());

        longTermSummaryRecords.add(SummaryRecord.builder().summaryTitle("ST to LT Book Holding Period Reclass")
                                                 .value(glSum.getSumGL())
                                                 .build());

        // todo - (NVL(v_ws_l_def_b_m,0)+NVL(v_sum_gl_b,0))-(v_sum_rev_adjst_l_b+SUM(t.cur_pr_lt_reversal_b))
        longTermSummaryRecords.add(SummaryRecord.builder().summaryTitle("LT Net Wash Sale Adjustment")
                                                .value(BigDecimal.valueOf(-1).multiply(glSum.getSumGL())) // todo
                                                .build());

        longTermSummaryRecords.add(SummaryRecord.builder().summaryTitle(dfFifthQuery.get(0).getTitle1())
                                                 .value(null)
                                                 .build());

        longTermSummaryRecords.add(SummaryRecord.builder().summaryTitle("Long Term Capital Gain")
                                                 .value(dfFifthQuery.stream()
                                                                    .map(SummaryRowData::getTitle1LongTermGainBase)
                                                                    .reduce(BigDecimal.ZERO, BigDecimal::add)) // todo
                                                 .build());

        // todo - NVL(v_ws_l_def_b,0)-SUM(t.long_term_loss_base)
        longTermSummaryRecords.add(SummaryRecord.builder().summaryTitle("Long Term Capital Loss")
                                                 .value(dfFifthQuery.stream()
                                                                    .map(SummaryRowData::getTitle1LongTermLossBase)
                                                                    .reduce(BigDecimal.ZERO, BigDecimal::add)) // todo - one field missing
                                                 .build());

        longTermSummaryRecords.add(SummaryRecord.builder().summaryTitle("Net Long Term Capital Gain/Loss") // todo - missing v_ws_s_def_b
                                                 .value(dfFifthQuery.stream()
                                                                    .map(x -> (x.getTitle1ShortTermGainBase().subtract(x.getTitle1ShortTermLossBase())))
                                                                    .reduce(BigDecimal.ZERO, BigDecimal::add))
                                                 .build());

        longTermSummaryRecords.add(SummaryRecord.builder().summaryTitle("Long Term Sale Proceeds")
                                                 .value(dfFifthQuery.stream()
                                                                    .map(SummaryRowData::getTitle1LongTermSaleBase)
                                                                    .reduce(BigDecimal.ZERO, BigDecimal::add))
                                                 .build());

        longTermSummaryRecords.add(SummaryRecord.builder().summaryTitle("Long Term Tax Cost") //todo missing v_ws_s_def_b
                                                 .value(dfFifthQuery.stream()
                                                                    .map(SummaryRowData::getTitle2LongTermBookCost)
                                                                    .reduce(BigDecimal.ZERO, BigDecimal::add))
                                                 .build());

        longTermSummaryRecords.add(SummaryRecord.builder().summaryTitle("Current Period Redemption in Kind (RIK) Activity")
                                                .value(null)
                                                .build());

        longTermSummaryRecords.add(SummaryRecord.builder().summaryTitle("LT Capital Reversal  - RIK")
                                                .value(BigDecimal.valueOf(-1).multiply(dfFifthQuery.stream()
                                                                                                   .map(SummaryRowData::getTitle3CurrentPeriodLTInKindReversalBase)
                                                                                                   .reduce(BigDecimal.ZERO, BigDecimal::add)))
                                                .build());

        longTermSummaryRecords.add(SummaryRecord.builder().summaryTitle("ST to LT Book Holding Period Reclass - RIK")
                                                .value(glSum.getSumGLInKind())
                                                .build());

        longTermSummaryRecords.add(SummaryRecord.builder().summaryTitle("LT Net Wash Sale Adjustment - RIK")
                                                .value(glSum.getSumGLInKind().add(BigDecimal.valueOf(-1)
                                                                                            .multiply(dfFifthQuery.stream()
                                                                                                                  .map(SummaryRowData::getTitle3CurrentPeriodLTInKindReversalBase)
                                                                                                                  .reduce(BigDecimal.ZERO, BigDecimal::add))))
                                                .build());

        longTermSummaryRecords.add(SummaryRecord.builder().summaryTitle("Net Holdings Tax Cost")
                                                .value(null)
                                                .build());

        longTermSummaryRecords.add(SummaryRecord.builder().summaryTitle("Total Book Cost")
                                                .value(dfFifthQuery.stream()
                                                                   .map(x -> x.getTitle2ShortTermBookCost().add(x.getTitle2LongTermBookCost()))
                                                                   .reduce(BigDecimal.ZERO, BigDecimal::add))
                                                .build());

        longTermSummaryRecords.add(SummaryRecord.builder().summaryTitle("Total Net WS Adjustments")
                                                .value(null)
                                                .build());

        BigDecimal totalBookCost = dfFifthQuery.stream().map(x -> x.getTitle2ShortTermBookCost().add(x.getTitle2LongTermBookCost()))
                                               .reduce(BigDecimal.ZERO, BigDecimal::add);
        BigDecimal ttc2nd =
            BigDecimal.ZERO.subtract(dfFifthQuery.stream().map(x -> x.getTitle3CurrentPeriodSTReversalBase().add(x.getTitle3CurrentPeriodSTInKindReversalBase()))
                                                 .reduce(BigDecimal.ZERO, BigDecimal::add));
        BigDecimal ttc3rd = BigDecimal.ZERO.subtract(dfFifthQuery.stream().map(x -> x.getTitle3CurrentPeriodLTReversalBase().add(x.getTitle3CurrentPeriodLTInKindReversalBase()))
                                                                 .reduce(BigDecimal.ZERO, BigDecimal::add));
        longTermSummaryRecords.add(SummaryRecord.builder().summaryTitle("Total Tax Cost") // todo - v_ws_s_def_b_m
                                                .value(totalBookCost.subtract(ttc2nd.add(ttc3rd)))
                                                .build());

        longTermSummaryRecords.add(SummaryRecord.builder().summaryTitle("DUMMY").value(null).build());
        longTermSummaryRecords.add(SummaryRecord.builder().summaryTitle("DUMMY").value(null).build());
        longTermSummaryRecords.add(SummaryRecord.builder().summaryTitle("DUMMY").value(null).build());
        longTermSummaryRecords.add(SummaryRecord.builder().summaryTitle("DUMMY").value(null).build());
        longTermSummaryRecords.add(SummaryRecord.builder().summaryTitle("DUMMY").value(null).build());

        // todo - some missing

        longTermSummaryRecords.add(SummaryRecord.builder().summaryTitle("Short Holdings Tax Proceeds")
                                                .value(null)
                                                .build());

        longTermSummaryRecords.add(SummaryRecord.builder().summaryTitle("Book Proceeds")
                                                .value(bookCostBase.getSumSTBookCost())
                                                .build());

        longTermSummaryRecords.add(SummaryRecord.builder().summaryTitle("Short Capital Deferrals")
                                                .value(bookCostBase.getSumSTWsDeferral())
                                                .build());

        longTermSummaryRecords.add(SummaryRecord.builder().summaryTitle("Tax Proceeds")
                                                .value(bookCostBase.getSumSTTaxCost())
                                                .build());
        return longTermSummaryRecords;
    }

    private static List<SummaryRecord> getShortTermSummaryRecords(ReversalAdjustment reversalAdjustment, GLSum glSum, BigDecimal priorRevision, BookCostBase bookCostBase,
        List<SummaryRowData> dfFifthQuery)
    {
        List<SummaryRecord> shortTermSummaryRecords = new ArrayList<>();

        //todo - Add Empty record
        shortTermSummaryRecords.add(SummaryRecord.builder().summaryTitle(dfFifthQuery.get(0).getTitle2())
                                                 .value(null)
                                                 .build());

        shortTermSummaryRecords.add(SummaryRecord.builder().summaryTitle("Short Term Capital Gain")
                                                 .value(dfFifthQuery.stream()
                                                                    .map(SummaryRowData::getTitle2ShortTermGainBase)
                                                                    .reduce(BigDecimal.ZERO, BigDecimal::add))
                                                 .build());
        shortTermSummaryRecords.add(SummaryRecord.builder().summaryTitle("Short Term Capital Loss")
                                                 .value(dfFifthQuery.stream()
                                                                    .map(SummaryRowData::getTitle2ShortTermLossBase)
                                                                    .reduce(BigDecimal.ZERO, BigDecimal::add))
                                                 .build());

        shortTermSummaryRecords.add(SummaryRecord.builder().summaryTitle("Net Short Term Capital Gain/Loss")
                                                 .value(dfFifthQuery.stream()
                                                                    .map(x -> (x.getTitle2ShortTermGainBase().subtract(x.getTitle2ShortTermLossBase())))
                                                                    .reduce(BigDecimal.ZERO, BigDecimal::add))
                                                 .build());

        shortTermSummaryRecords.add(SummaryRecord.builder().summaryTitle("Short Term Sale Proceeds")
                                                 .value(dfFifthQuery.stream()
                                                                    .map(SummaryRowData::getTitle2ShortTermSaleBase)
                                                                    .reduce(BigDecimal.ZERO, BigDecimal::add))
                                                 .build());
        shortTermSummaryRecords.add(SummaryRecord.builder().summaryTitle("Short Term Book Cost")
                                                 .value(dfFifthQuery.stream()
                                                                    .map(SummaryRowData::getTitle2ShortTermBookCost)
                                                                    .reduce(BigDecimal.ZERO, BigDecimal::add))
                                                 .build());

        shortTermSummaryRecords.add(SummaryRecord.builder().summaryTitle(dfFifthQuery.get(0).getTitle3())
                                                 .value(null)
                                                 .build());

        shortTermSummaryRecords.add(SummaryRecord.builder().summaryTitle("ST Capital Deferrals")
                                                 .value(BigDecimal.ZERO) // todo - not calculated
                                                 .build());

        shortTermSummaryRecords.add(SummaryRecord.builder().summaryTitle("ST Capital Reversals")
                                                 .value(BigDecimal.valueOf(-1).multiply(dfFifthQuery.stream()
                                                                                                    .map(SummaryRowData::getTitle3CurrentPeriodSTReversalBase)
                                                                                                    .reduce(BigDecimal.ZERO, BigDecimal::add).add(reversalAdjustment.getShortTermReversalAdjustment())))
                                                 .build());

        shortTermSummaryRecords.add(SummaryRecord.builder().summaryTitle("ST to LT Book Holding Period Reclass")
                                                 .value(BigDecimal.valueOf(-1).multiply(glSum.getSumGL()))
                                                 .build());

        shortTermSummaryRecords.add(SummaryRecord.builder().summaryTitle("ST Net Wash Sale Adjustment")
                                                 .value(BigDecimal.valueOf(-1).multiply(glSum.getSumGL())) // todo
                                                 .build());

        shortTermSummaryRecords.add(SummaryRecord.builder().summaryTitle(dfFifthQuery.get(0).getTitle1())
                                                 .value(null)
                                                 .build());

        shortTermSummaryRecords.add(SummaryRecord.builder().summaryTitle("Short Term Capital Gain")
                                                 .value(dfFifthQuery.stream()
                                                                    .map(SummaryRowData::getTitle1ShortTermGainBase)
                                                                    .reduce(BigDecimal.ZERO, BigDecimal::add)) // todo
                                                 .build());

        shortTermSummaryRecords.add(SummaryRecord.builder().summaryTitle("Short Term Capital Loss")
                                                 .value(dfFifthQuery.stream()
                                                                    .map(SummaryRowData::getTitle1ShortTermLossBase)
                                                                    .reduce(BigDecimal.ZERO, BigDecimal::add)) // todo - one field missing
                                                 .build());

        shortTermSummaryRecords.add(SummaryRecord.builder().summaryTitle("Net Short Term Capital Gain/Loss") // todo - missing v_ws_s_def_b
                                                 .value(dfFifthQuery.stream()
                                                                    .map(x -> (x.getTitle1ShortTermGainBase().subtract(x.getTitle1ShortTermLossBase())))
                                                                    .reduce(BigDecimal.ZERO, BigDecimal::add))
                                                 .build());

        shortTermSummaryRecords.add(SummaryRecord.builder().summaryTitle("Short Term Sale Proceeds")
                                                 .value(dfFifthQuery.stream()
                                                                    .map(SummaryRowData::getTitle1ShortTermSaleBase)
                                                                    .reduce(BigDecimal.ZERO, BigDecimal::add))
                                                 .build());
        shortTermSummaryRecords.add(SummaryRecord.builder().summaryTitle("Short Term Tax Cost") //todo missing v_ws_s_def_b
                                                 .value(dfFifthQuery.stream()
                                                                    .map(SummaryRowData::getTitle2ShortTermBookCost)
                                                                    .reduce(BigDecimal.ZERO, BigDecimal::add))
                                                 .build());


        shortTermSummaryRecords.add(SummaryRecord.builder().summaryTitle("Current Period Redemption in Kind (RIK) Activity")
                                                 .value(null)
                                                 .build());

        shortTermSummaryRecords.add(SummaryRecord.builder().summaryTitle("ST Capital Reversal  - RIK")
                                                 .value(BigDecimal.valueOf(-1).multiply(dfFifthQuery.stream()
                                                                                                    .map(SummaryRowData::getTitle3CurrentPeriodSTInKindReversalBase)
                                                                                                    .reduce(BigDecimal.ZERO, BigDecimal::add)))
                                                 .build());

        shortTermSummaryRecords.add(SummaryRecord.builder().summaryTitle("ST to LT Book Holding Period Reclass - RIK")
                                                 .value(BigDecimal.valueOf(-1).multiply(glSum.getSumGLInKind()))
                                                 .build());

        // todo
        shortTermSummaryRecords.add(SummaryRecord.builder().summaryTitle("ST Net Wash Sale Adjustment - RIK")
                                                 .value(BigDecimal.valueOf(-1).multiply(glSum.getSumGLInKind()))
                                                 .build());

        shortTermSummaryRecords.add(SummaryRecord.builder().summaryTitle("Wash Sale Roll Forward")
                                                 .value(null)
                                                 .build());

        shortTermSummaryRecords.add(SummaryRecord.builder().summaryTitle("Prior Period Deferrals") // todo - not found v_prior_def_b
                                                 .value(priorRevision)
                                                 .build());

        shortTermSummaryRecords.add(SummaryRecord.builder().summaryTitle("Converted Wash Sales - Fund Merger") // todo - not found v_ws_def_merger_b
                                                 .value(priorRevision)
                                                 .build());

        shortTermSummaryRecords.add(SummaryRecord.builder().summaryTitle("ST Net Wash Sale Adjustment") //todo - v_ws_l_def_b_m
                                                 .value(glSum.getSumGL()
                                                             .subtract(reversalAdjustment.getShortTermReversalAdjustment()
                                                                                         .add(dfFifthQuery.stream()
                                                                                                          .map(SummaryRowData::getTitle3CurrentPeriodSTReversalBase)
                                                                                                          .reduce(BigDecimal.ZERO, BigDecimal::add))))
                                                 .build());

        shortTermSummaryRecords.add(SummaryRecord.builder().summaryTitle("LT Net Wash Sale Adjustment") //todo - v_ws_l_def_b_m
                                                 .value(glSum.getSumGL()
                                                             .subtract(reversalAdjustment.getLongTermReversalAdjustment()
                                                                                         .add(dfFifthQuery.stream()
                                                                                                          .map(SummaryRowData::getTitle3CurrentPeriodLTReversalBase)
                                                                                                          .reduce(BigDecimal.ZERO, BigDecimal::add))))
                                                 .build());

        shortTermSummaryRecords.add(SummaryRecord.builder().summaryTitle("ST Net Wash Sale Adjustment - RIK")
                                                 .value(dfFifthQuery.stream()
                                                                    .map(x -> BigDecimal.valueOf(-1)
                                                                                        .multiply(x.getTitle3CurrentPeriodSTInKindReversalBase())
                                                                                        .subtract(glSum.getSumGLInKind()))
                                                                    .reduce(BigDecimal.ZERO, BigDecimal::add))
                                                 .build());

        shortTermSummaryRecords.add(SummaryRecord.builder().summaryTitle("LT Net Wash Sale Adjustment - RIK")
                                                 .value(glSum.getSumGLInKind().add(BigDecimal.valueOf(-1)
                                                                                             .multiply(dfFifthQuery.stream()
                                                                                                                   .map(SummaryRowData::getTitle3CurrentPeriodLTInKindReversalBase)
                                                                                                                   .reduce(BigDecimal.ZERO, BigDecimal::add))))
                                                 .build());

        shortTermSummaryRecords.add(SummaryRecord.builder().summaryTitle("Total Open WS Deferrals")
                                                 .value(null) // todo
                                                 .build());

        shortTermSummaryRecords.add(SummaryRecord.builder().summaryTitle("Post 30 Day Wash Sale Deferrals")
                                                 .value(null) // todo not available v_post_def_b
                                                 .build());

        // todo - some missing calcs

        shortTermSummaryRecords.add(SummaryRecord.builder().summaryTitle("Long Holdings Tax Cost")
                                                .value(null)
                                                .build());

        shortTermSummaryRecords.add(SummaryRecord.builder().summaryTitle("Book Cost")
                                                .value(bookCostBase.getSumLTBookCost())
                                                .build());

        shortTermSummaryRecords.add(SummaryRecord.builder().summaryTitle("Long Capital Deferrals")
                                                .value(bookCostBase.getSumLTWsDeferral())
                                                .build());

        shortTermSummaryRecords.add(SummaryRecord.builder().summaryTitle("Tax Cost")
                                                .value(bookCostBase.getSumLTTaxCost())
                                                .build());
        return shortTermSummaryRecords;
    }

    private static ReversalAdjustment executeReversalAdjQuery(Session session)
    {
        // Define Tables to be queried
        DataFrame tradeTable = session.table("TRADE_WITH_WS_POC_TEST");
        DataFrame lotLevelTable = session.table("LOT_LEVEL_WS_POSITION_POC_TEST");
        DataFrame secMasterTable = session.table("SECURITY_MASTER_POC_TEST");

        // Define joins
        DataFrame queryWithWhereClause = tradeTable
            .join(lotLevelTable, tradeTable.col("entity_id").equal_to(lotLevelTable.col("entity_id"))
                                           .and(tradeTable.col("src_intfc_inst").equal_to(lotLevelTable.col("src_intfc_inst")))
                                           .and(tradeTable.col("long_short_ind").equal_to(lotLevelTable.col("long_short_ind")))
                                           .and(tradeTable.col("target_event_id").equal_to(lotLevelTable.col("orig_lot_number")))
                                           //.and(tradeTable.col("orig_acct_date").equal_to(lotLevelTable.col("effective_date")))
                                           .and(tradeTable.col("security_alias").equal_to(lotLevelTable.col("security_alias"))))
            .join(secMasterTable, tradeTable.col("security_alias").equal_to(secMasterTable.col("security_alias")))
            .filter(Functions.trim(tradeTable.col("entity_id"), Functions.lit(" ")).equal_to(Functions.lit("BA32")))
            .filter(tradeTable.col("acct_basis").equal_to(Functions.lit("USTAX")))
            .filter(tradeTable.col("ws_dis_reversal_b").not_equal(Functions.lit(0)))
            .filter(tradeTable.col("cancel_status").equal_to(Functions.lit("N")))
            .filter(tradeTable.col("record_type").in(35000, 45000));

        //Row[] rows = queryWithWhereClause.collect();
        //log.info("Record Count >> " + rows.length);
        // Select columns needed from the tables
        DataFrame finalData = queryWithWhereClause.select(
            Functions.sum(
                Functions.iff(
                    lotLevelTable.col("ws_disallow_term").equal_to(Functions.lit("L")),
                    tradeTable.col("ws_dis_reversal_b"),
                    Functions.lit(0)
                )).alias("lt_reversal_adjst"),
            Functions.sum(
                Functions.iff(
                    lotLevelTable.col("ws_disallow_term").equal_to(Functions.lit("S")),
                    tradeTable.col("ws_dis_reversal_b"),
                    Functions.lit(0)
                )).alias("st_reversal_adjst"));

        Row[] row = finalData.collect();
        log.info("Record Count >> " + row.length);
        return ReversalAdjustment.builder()
                                 .longTermReversalAdjustment((BigDecimal) row[0].get(0))
                                 .shortTermReversalAdjustment((BigDecimal) row[0].get(1))
                                 .build();
    }

    private static GLSum executeGLSum(Session session)
    {
        // Define Tables to be queried
        DataFrame tradeTable = session.table("trade_poc_test");
        DataFrame tradeDetailTable = session.table("trade_detail_poc_test");
        DataFrame disposalLotsTable = session.table("disposal_lots_poc_test");
        DataFrame disposalLotWsTable = session.table("disposal_lot_ws_poc_test");
        DataFrame secMasterTable = session.table("security_master_poc_test");

        // Define joins
        DataFrame tradeTableJoins =
            tradeTable.join(tradeDetailTable, tradeTable.col("trade_id").equal_to(tradeDetailTable.col("trade_id")))
                      .join(disposalLotsTable, tradeTable.col("trade_id").equal_to(disposalLotsTable.col("trade_id")), "right")
                      .join(secMasterTable, tradeTable.col("security_alias").equal_to(secMasterTable.col("security_alias")));

        DataFrame disposalLotJoin =
            tradeTableJoins.join(disposalLotWsTable,
                tradeTableJoins.col("disposal_lot_id").equal_to(disposalLotWsTable.col("disposal_lot_id")), "right");

        DataFrame queryWithWhereClause =
            disposalLotJoin.filter(Functions.trim(tradeTable.col("entity_id"), Functions.lit(" ")).equal_to(Functions.lit("BA32")))
                           .filter(tradeTable.col("acct_basis").equal_to(Functions.lit("USTAX")))
                           .filter(tradeDetailTable.col("cancel_flag").not_equal(Functions.lit("Y")))
                           .filter(tradeDetailTable.col("cancel_status").not_equal(Functions.lit("Y")))
                           .filter(disposalLotWsTable.col("gain_loss_term_reclass").equal_to(Functions.lit("Y")));

        //Row[] rows = queryWithWhereClause.collect();

        Column bookGainB = Functions.iff(
            Functions.is_null(disposalLotWsTable.col("book_gain_b")),
            Functions.lit(0),
            disposalLotWsTable.col("book_gain_b"));

        Column bookLossB = Functions.iff(
            Functions.is_null(disposalLotWsTable.col("book_loss_b")),
            Functions.lit(0),
            disposalLotWsTable.col("book_loss_b"));

        Column wsReversalType = Functions.iff(
            Functions.is_null(disposalLotWsTable.col("ws_reversal_type")),
            Functions.lit("X"),
            disposalLotWsTable.col("ws_reversal_type"));

        Column notInKind = Functions.iff(
            wsReversalType.not_equal(Functions.lit("INKIND")),
            bookGainB.minus(bookLossB),
            Functions.lit(0));

        Column inKind = Functions.iff(
            wsReversalType.equal_to(Functions.lit("INKIND")),
            bookGainB.minus(bookLossB),
            Functions.lit(0));

        // Select columns needed from the tables
        Row[] rows = queryWithWhereClause.select(
            Functions.sum(notInKind).alias("v_sum_gl_b"),
            Functions.sum(inKind).alias("v_sum_gl_b_ink")).collect();

        log.info("Record Count >> " + rows.length);

        return GLSum.builder()
                    .sumGL((BigDecimal) rows[0].get(0))
                    .sumGLInKind((BigDecimal) rows[0].get(1))
                    .build();

    }

    private static BigDecimal executePriorRevisionQuery(Session session)
    {
        // Define Tables to be queried
        DataFrame tradeTable = session.table("trade_poc_test");
        DataFrame tradeDetailTable = session.table("trade_detail_poc_test");
        DataFrame disposalLotsTable = session.table("disposal_lots_poc_test");
        DataFrame disposalLotWsTable = session.table("disposal_lot_ws_poc_test");
        DataFrame secMasterTable = session.table("SECURITY_MASTER_POC_TEST");

        // Define joins
        DataFrame tradeTableJoins =
            tradeTable.join(tradeDetailTable, tradeTable.col("trade_id").equal_to(tradeDetailTable.col("trade_id")))
                      .join(disposalLotsTable, tradeTable.col("trade_id").equal_to(disposalLotsTable.col("trade_id")), "right")
                      .join(secMasterTable, tradeTable.col("security_alias").equal_to(secMasterTable.col("security_alias")));

        DataFrame disposalLotJoin =
            tradeTableJoins.join(disposalLotWsTable,
                tradeTableJoins.col("disposal_lot_id").equal_to(disposalLotWsTable.col("disposal_lot_id")), "right");

        DataFrame queryWithWhereClause =
            disposalLotJoin.filter(Functions.trim(tradeTable.col("entity_id"), Functions.lit(" ")).equal_to(Functions.lit("BA32")))
                           .filter(tradeTable.col("acct_basis").equal_to(Functions.lit("USTAX")))
                           .filter(tradeTable.col("ledger_effective_date").leq(Functions.to_date(Functions.lit("20230101"), Functions.lit("YYYYMMDD")))) // sysdate for
                           // now
                           .filter(tradeDetailTable.col("cancel_flag").not_equal(Functions.lit("Y")))
                           .filter(tradeDetailTable.col("cancel_status").not_equal(Functions.lit("Y")));


        Column wsDisReversal = Functions.iff(
            disposalLotWsTable.col("ws_dis_reversal_b").is_null(),
            Functions.lit(0),
            disposalLotWsTable.col("ws_dis_reversal_b"));

        // Select columns needed from the tables
        Row[] rows = queryWithWhereClause.select(Functions.sum(wsDisReversal).alias("lt_reversal_adjst")).collect();
        log.info("Record Count >> " + rows.length);

        return (BigDecimal) rows[0].get(0);
    }

    private static BookCostBase executeBookCostBaseQuery(Session session)
    {
        String sql = "\n" +
            " -- to get total book cost base from lot level position report\n" +
            "  SELECT NVL(SUM(CASE WHEN trim(pd.long_short_ind) = 'L' THEN NVL(pcl.book_cost_b,0) ELSE 0 END),0) v_sum_l_bookcost_b,  --\"Total LT Book Cost Base\"\n" +
            "         NVL(SUM(CASE WHEN trim(pd.long_short_ind) = 'S' THEN NVL(abs(pcl.book_cost_b),0) ELSE 0 END),0) v_sum_s_bookcost_b,  --\"Total ST Book Cost " +
            "Base\"\n" +
            "         NVL(SUM(CASE WHEN trim(pd.long_short_ind) = 'L' THEN NVL(llwsp.curr_ws_dis_loss_b,0) ELSE 0 END),0) v_sum_l_ws_def_b,  --\"Total LT Open WS " +
            "Deferral Base\"\n" +
            "         NVL(SUM(CASE WHEN trim(pd.long_short_ind) = 'S' THEN NVL(llwsp.curr_ws_dis_loss_b,0) ELSE 0 END),0) v_sum_s_ws_def_b,  --\"Total ST Open WS " +
            "Deferral Base\"\n" +
            "         NVL(SUM(CASE WHEN trim(pd.long_short_ind) = 'L' THEN NVL(pcl.book_cost_b,0)+NVL(llwsp.curr_ws_dis_loss_b,0) ELSE 0 END),0) v_sum_l_taxcost_b, " +
            "--\"Total LT Tax Cost Base\"\n" +
            "         NVL(SUM(CASE WHEN trim(pd.long_short_ind) = 'S' THEN NVL(abs(pcl.book_cost_b),0)-NVL(llwsp.curr_ws_dis_loss_b,0) ELSE 0 END),0) v_sum_s_taxcost_b" +
            " --\"Total ST Tax Cost Base\"\n" +
            "    --INTO v_sum_l_bookcost_b, v_sum_s_bookcost_b, v_sum_l_ws_def_b, v_sum_s_ws_def_b, v_sum_l_taxcost_b, v_sum_s_taxcost_b\n" +
            "    FROM position_poc_test p,\n" +
            "         position_detail_poc_test pd,\n" +
            "         lot_level_position_poc_test llp,\n" +
            "         position_cost_lot_poc_test pcl,\n" +
            "         lot_level_ws_position_poc_test llwsp,\n" +
            "         security_master_poc_test sm\n" +
            "   WHERE trim(p.entity_id) = 'BA32'\n" +
            "     and p.position_id = pd.position_id \n" +
            "     and pd.position_id = llp.position_id \n" +
            "     and pd.long_short_ind = llp.long_short_ind \n" +
            "     and pd.security_alias = llp.security_alias \n" +
            "     and pd.security_alias = sm.security_alias \n" +
            "     and llp.lot_level_position = pcl.position_lot_id \n" +
            "     and llp.lot_level_position = llwsp.lot_level_position(+) \n" +
            "     and p.acct_basis = 'USTAX'";

        DataFrame select = session.sql(sql);

        Row[] rows = select.collect();
        log.info("Record Count >> " + rows.length);

        // Select columns needed from the tables
        return BookCostBase.builder()
                           .sumLTBookCost((BigDecimal) rows[0].get(0))
                           .sumSTBookCost((BigDecimal)rows[0].get(1))
                           .sumLTWsDeferral((BigDecimal)rows[0].get(2))
                           .sumSTWsDeferral((BigDecimal)rows[0].get(3))
                           .sumLTTaxCost((BigDecimal)rows[0].get(4))
                           .sumSTTaxCost((BigDecimal)rows[0].get(5))
                           .build();
    }

    private static List<SummaryRowData> executeSummaryRowCollection(Session session)
    {
        String sql = "  -- fetching rows for summary report and load into collection object\n" +
            "  SELECT NVL(t.base_currency,'NA'),\n" +
            "                     NVL(t.entity_id,'NA'),\n" +
            "                     -- Tax RGL\n" +
            "                     'Tax Realized Gain Loss Summary',            --title 1\n" +
            "                     NVL(CASE WHEN trim(dl.gain_loss_term) = 'S' THEN NVL(dl.gain_security,0) ELSE 0 END,0), --short term gain base\n" +
            "                     NVL(CASE WHEN trim(dl.gain_loss_term) = 'S' THEN NVL(dl.loss_security,0) ELSE 0 END,0), --short term loss base\n" +
            "                     NVL(CASE WHEN trim(dl.gain_loss_term) = 'L' THEN NVL(dl.gain_security,0) ELSE 0 END,0), --long_term_gain_base\n" +
            "                     NVL(CASE WHEN trim(dl.gain_loss_term) = 'L' THEN NVL(dl.loss_security,0) ELSE 0 END,0), --long term loss base\n" +
            "                     NVL(CASE WHEN trim(dl.gain_loss_term) = 'S' THEN NVL(decode(trim(dl.long_short_indicator),'L',dl.settleamt0+NVL(dl.premium_allocated," +
            "0),dl.book_cost_b),0) \n" +
            "                         ELSE 0 END,0),  --short_term_sale_base\n" +
            "                     NVL(CASE WHEN trim(dl.gain_loss_term) = 'S' THEN CASE WHEN (dl.trans_code = 'PENDING_LOSS') THEN NVL(dl.book_cost_b,0)\n" +
            "                         ELSE NVL(decode(trim(dl.long_short_indicator),'L',dl.book_value,NVL(dl.settleamt0,0)+NVL(dl.premium_allocated,0)+NVL(dws" +
            ".ws_dis_reversal_b,0)),0) END \n" +
            "                         ELSE 0 END,0),  --short term tax cost b\n" +
            "                     NVL(CASE WHEN trim(dl.gain_loss_term) = 'L' THEN NVL(dl.settleamt0,0)+NVL(dl.premium_allocated,0) ELSE 0 END,0),  --long term sale " +
            "base\n" +
            "                     NVL(CASE WHEN trim(dl.gain_loss_term) = 'L' THEN CASE WHEN (dl.trans_code = 'PENDING_LOSS') THEN NVL(dl.book_cost_b,0) \n" +
            "                         ELSE NVL(dl.book_value,0) END ELSE 0 END,0),  --long term tax cost b\n" +
            "                     -- Book RGL\n" +
            "                     'Book Realized Gain Loss Summary',           --title 2\n" +
            "                     NVL(CASE WHEN trim(dl.gain_loss_term_book) = 'S' THEN NVL(dl.book_gain_b,0) ELSE 0 END,0), --short term gain base\n" +
            "                     NVL(CASE WHEN trim(dl.gain_loss_term_book) = 'S' THEN NVL(dl.book_loss_b,0) ELSE 0 END,0), --short term loss base\n" +
            "                     NVL(CASE WHEN trim(dl.gain_loss_term_book) = 'L' THEN NVL(dl.book_gain_b,0) ELSE 0 END,0), --long_term_gain_base\n" +
            "                     NVL(CASE WHEN trim(dl.gain_loss_term_book) = 'L' THEN NVL(dl.book_loss_b,0) ELSE 0 END,0), --long term loss base\n" +
            "                     NVL(CASE WHEN trim(dl.gain_loss_term_book) = 'S' THEN NVL(decode(trim(dl.long_short_indicator),'L',dl.settleamt0+NVL(dl" +
            ".premium_allocated,0),dl.book_cost_b),0) \n" +
            "                         ELSE 0 END,0),  --short_term_sale_base\n" +
            "                     NVL(CASE WHEN trim(dl.gain_loss_term_book) = 'S' THEN NVL(decode(trim(dl.long_short_indicator),'L',dl.book_cost_b,dl.settleamt0+NVL" +
            "(dl.premium_allocated,0)),0) \n" +
            "                         ELSE 0 END,0), --short term book cost b\n" +
            "                     NVL(CASE WHEN trim(dl.gain_loss_term_book) = 'L' THEN NVL(dl.settleamt0,0)+NVL(dl.premium_allocated,0) ELSE 0 END,0),  --long term " +
            "sale base\n" +
            "                     NVL(CASE WHEN trim(dl.gain_loss_term_book) = 'L' THEN NVL(dl.book_cost_b,0) ELSE 0 END,0), --long term book cost b\n" +
            "                     -- Wash Sale\n" +
            "                     'Current Period Wash Sale Activity',                         --title 3\n" +
            "                     NVL(CASE WHEN dws.gain_loss_type = 'S' THEN NVL(dws.ws_dis_reversal_b,0) ELSE 0 END,0), --current period ST deferral base\n" +
            "                     NVL(CASE WHEN (dws.gain_loss_type = 'S' and NVL(dws.ws_reversal_type,'X') != 'INKIND')\n" +
            "                        THEN NVL(dws.ws_dis_reversal_b,0) ELSE 0 END,0), --current period ST reversal base\n" +
            "                     NVL(CASE WHEN (dws.gain_loss_type = 'S' and dws.ws_reversal_type = 'INKIND') THEN\n" +
            "                        NVL(dws.ws_dis_reversal_b, 0) ELSE 0 END,0),     --current period ST inkind reversal base\n" +
            "                     NVL(CASE WHEN dws.gain_loss_type = 'L' THEN NVL(dws.ws_dis_reversal_b,0) ELSE 0 END,0), --current period LT deferral base\n" +
            "                     NVL(CASE WHEN (dws.gain_loss_type = 'L' and NVL(dws.ws_reversal_type,'X') != 'INKIND')\n" +
            "                        THEN NVL(dws.ws_dis_reversal_b,0) ELSE 0 END,0), --current period LT reversal base\n" +
            "                     NVL(CASE WHEN (dws.gain_loss_type = 'L' and dws.ws_reversal_type = 'INKIND')\n" +
            "                        THEN NVL(dws.ws_dis_reversal_b, 0) ELSE 0 END,0) --current period LT inkind reversal base\n" +
            "    FROM  trade_poc_test t,\n" +
            "          trade_detail_poc_test trd,\n" +
            "          disposal_lots_poc_test dl,\n" +
            "          disposal_lot_ws_poc_test dws,\n" +
            "          security_master_poc_test sm \n" +
            "    WHERE trim(t.entity_id) = 'BA32'\n" +
            "      and t.trade_id = trd.trade_id \n" +
            "      and t.trade_id = dl.trade_id(+) \n" +
            "      and dl.disposal_lot_id = dws.disposal_lot_id(+) \n" +
            "      and t.security_alias = sm.security_alias \n" +
            "      and t.acct_basis = 'USTAX'\n" +
            "      and t.ledger_effective_date >= to_date('20150101','YYYYMMDD') \n" +
            "      and t.ledger_effective_date <= to_date('20230101','YYYYMMDD')\n" +
            "      and NVL(trim(trd.cancel_flag),'X') != 'Y' \n" +
            "      and NVL(trim(trd.cancel_status),'X') != 'Y';\n";

        DataFrame select = session.sql(sql);

        Row[] rows = select.collect();
        log.info("Record Count >> " + rows.length);

        return Arrays.stream(rows).map(x -> SummaryRowData.builder()
                                                          .baseCurrency((String)x.get(0))
                                                          .entityId((String)x.get(1))
                                                          .title1((String)x.get(2))
                                                          .title1ShortTermGainBase((BigDecimal) x.get(3))
                                                          .title1ShortTermLossBase((BigDecimal) x.get(4))
                                                          .title1LongTermGainBase((BigDecimal) x.get(5))
                                                          .title1LongTermLossBase((BigDecimal) x.get(6))
                                                          .title1ShortTermSaleBase((BigDecimal) x.get(7))
                                                          .title1ShortTermTaxCost((BigDecimal) x.get(8))
                                                          .title1LongTermSaleBase((BigDecimal) x.get(9))
                                                          .title1LongTermTaxCost((BigDecimal) x.get(10))
                                                          .title2((String)x.get(11))
                                                          .title2ShortTermGainBase((BigDecimal)x.get(12))
                                                          .title2ShortTermLossBase((BigDecimal)x.get(13))
                                                          .title2LongTermGainBase((BigDecimal)x.get(14))
                                                          .title2LongTermLossBase((BigDecimal)x.get(15))
                                                          .title2ShortTermSaleBase((BigDecimal)x.get(16))
                                                          .title2ShortTermBookCost((BigDecimal)x.get(17))
                                                          .title2LongTermSaleBase((BigDecimal)x.get(18))
                                                          .title2LongTermBookCost((BigDecimal)x.get(19))
                                                          .title3((String)x.get(20))
                                                          .title3CurrentPeriodSTDeferralBase((BigDecimal)x.get(21))
                                                          .title3CurrentPeriodSTReversalBase((BigDecimal)x.get(22))
                                                          .title3CurrentPeriodSTInKindReversalBase((BigDecimal)x.get(23))
                                                          .title3CurrentPeriodLTDeferralBase((BigDecimal)x.get(24))
                                                          .title3CurrentPeriodLTReversalBase((BigDecimal)x.get(25))
                                                          .title3CurrentPeriodLTInKindReversalBase((BigDecimal)x.get(26))
                                                          .build())
                     .collect(Collectors.toList());
    }
}