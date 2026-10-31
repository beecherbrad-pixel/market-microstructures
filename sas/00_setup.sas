/*---------------------------------------------------------------------------
  00_setup.sas - paths, libraries and data import for the SAS replication.

  Expects the CSVs written by notebooks/03_price_impact.py in
      ~/microstructure/exports/
  in SAS OnDemand for Academics (upload them in SAS Studio: Server Files and
  Folders -> Files (Home) -> new folder "microstructure/exports" -> Upload).

  Writes SAS datasets to ~/microstructure/sasdata and results CSVs to
  ~/microstructure/sas_output.
---------------------------------------------------------------------------*/

%let root    = %sysget(HOME)/microstructure;
%let exports = &root/exports;
%let out     = &root/sas_output;

/* DLCREATEDIR makes the folders if they do not exist yet. */
/* V7 names: the macros in 02_volatility.sas build variable names such as
   b_hat_log_volume, which need standard SAS naming rules. */
options validvarname=v7 dlcreatedir nonotes;
libname micro "&root/sasdata";
libname _mkout "&out";
libname _mkout clear;
options notes;

/* Minute bars in regression units, one row per pair and minute.
   An explicit INPUT statement instead of PROC IMPORT, so the column types do
   not depend on what SAS guesses from the first rows. */
data micro.regression_bars;
    infile "&exports/regression_bars.csv" dsd firstobs=2 truncover;
    length symbol $12;
    input symbol $
          trade_date    :yymmdd10.
          bar_start_utc :anydtdtm19.
          ret_bps
          flow_musd
          n_trades
          volume_musd;
    format trade_date yymmdd10. bar_start_utc datetime19.;
    label ret_bps   = "One-minute log return, basis points"
          flow_musd = "Net signed quote volume, millions of USDT";
run;

/* Daily panel behind the second-stage regressions: one row per pair-day,
   already filtered (n_obs >= 1000) and with the log, lag and split-sample
   variables built in Python. */
proc import datafile="&exports/impact_panel.csv"
            out=micro.impact_panel dbms=csv replace;
    guessingrows=max;
run;

proc sort data=micro.regression_bars;
    by symbol trade_date bar_start_utc;
run;

proc sort data=micro.impact_panel;
    by symbol trade_date;
run;

title "Imported data";
proc sql;
    select symbol,
           count(distinct trade_date) as days,
           count(*) as bars format=comma12.
    from micro.regression_bars
    group by symbol;

    select symbol, count(*) as pair_days
    from micro.impact_panel
    group by symbol;
quit;
title;
