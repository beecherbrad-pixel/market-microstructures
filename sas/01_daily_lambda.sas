/*---------------------------------------------------------------------------
  01_daily_lambda.sas - Kyle's lambda, one regression per pair and UTC day.

      ret_bps_t = alpha + lambda * flow_musd_t + e_t

  HCC HCCMETHOD=1 gives White's HC1 standard error, n/(n-k) times the
  sandwich, which is what microstructure.impact.kyle_lambda computes in Spark.

  Output: micro.sas_lambda_daily and &out/sas_lambda_daily.csv
---------------------------------------------------------------------------*/

/* 180 BY groups would print 180 sets of tables; keep them out of the results
   window. ODS OUTPUT still captures the tables. */
ods exclude all;
ods output ParameterEstimates=work._lam_pe;

proc reg data=micro.regression_bars outest=work._lam_fit rsquare edf;
    by symbol trade_date;
    model ret_bps = flow_musd / hcc hccmethod=1;
run;
quit;

ods exclude none;

/* ParameterEstimates has one row per coefficient; keep the slope, and take
   the intercept, R-squared and degrees of freedom from OUTEST. The HC
   columns (HCStdErr, HCtValue, HCProbt) keep their SAS names. */
data micro.sas_lambda_daily;
    merge work._lam_pe(where=(upcase(Variable) = "FLOW_MUSD")
                       keep=symbol trade_date Variable Estimate StdErr tValue HC:)
          work._lam_fit(keep=symbol trade_date Intercept _RSQ_ _EDF_ _P_);
    by symbol trade_date;
    n_obs = _EDF_ + _P_;
    rename Estimate = lambda
           StdErr   = lambda_se
           tValue   = lambda_t
           Intercept = alpha
           _RSQ_    = r2;
    drop Variable _EDF_ _P_;
    format _numeric_ best16. trade_date yymmdd10.;
run;

proc export data=micro.sas_lambda_daily
            outfile="&out/sas_lambda_daily.csv" dbms=csv replace;
run;

title "Daily lambda (bps per $1M of net buying), by pair";
proc means data=micro.sas_lambda_daily n median p25 p75 mean maxdec=3;
    class symbol;
    var lambda r2 n_obs;
run;
title;
