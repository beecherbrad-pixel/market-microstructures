/*---------------------------------------------------------------------------
  02_volatility.sas - does price impact rise on volatile days?

  Re-estimates every second-stage model in src/microstructure/analysis.py on
  the daily panel, with the same samples and standard errors:

    pooled panel   PROC SURVEYREG, pair fixed effects (CLASS symbol),
                   standard errors clustered by date (CLUSTER trade_date).
                   SURVEYREG's default adjustment, G/(G-1) * (n-1)/(n-p), is
                   the one statsmodels applies to clustered errors.
    each pair      PROC AUTOREG, Newey-West standard errors with 5 lags:
                   COVEST=HAC with the Bartlett kernel. SAS weights lag j by
                   1 - j/b, statsmodels by 1 - j/(lags+1), so BANDWIDTH=6.
                   Neither applies a degrees-of-freedom correction.
    IV model       two-stage least squares in two passes, as in analysis.py:
                   first stages save fitted values, the second stage gives the
                   coefficients, and a third regression on
                   y* = y - sum_k (x_k - xhat_k) * b_k  gives standard errors
                   built from the structural residuals.

  Also, as cross-checks on the coefficients only:
    PROC PANEL  (FIXONE)  the pooled log model, classical standard errors
    PROC SYSLIN (2SLS)    the IV model per pair, classical standard errors

  SAS reports t-based p-values and statsmodels normal-based ones for the
  robust errors, so the p-values differ slightly even where coefficients and
  standard errors agree.

  Output: micro.sas_results and &out/sas_results.csv, one row per model,
  sample and slope, in the same layout as python_results.csv.
---------------------------------------------------------------------------*/

%let hac_lags = 5;
%let hac_bandwidth = %eval(&hac_lags + 1);

data work.results;
    length model $24 sample $12 term $32 cov_type $16;
    call missing(model, sample, term, cov_type);
    call missing(coef, se, t, p_value, n_obs, r2, first_stage_f);
    stop;
run;

/* Rows with every variable present; _grp is the BY group: "pooled", or the
   pair when each pair gets its own regression. */
%macro _sample(vars, pooled);
    data work._d;
        set micro.impact_panel;
        if nmiss(of &vars) = 0;
        length _grp $12;
        _grp = %if &pooled %then "pooled"; %else symbol;;
    run;
%mend;

/* Newey-West Wald test that every slope is zero, one per _grp, as an F
   statistic (Wald / number of slopes). Same estimator as COVEST=HAC with the
   Bartlett kernel and no degrees-of-freedom adjustment:
       V = inv(X'X) S inv(X'X),  S = sum_t g_t g_t' + sum_{j<b} (1 - j/b) (G_j + G_j'),
   with g_t = x_t e_t, G_j = sum_t g_t g_{t-j}' and b = &hac_bandwidth.
   Used for the per-pair first stages, where the slopes are the instruments.
   Rows must be in time order within each _grp; work._d is. */
%macro _hac_wald(y, xs, out);
    proc iml;
        use work._d;
        read all var {&xs} into Z;
        read all var {&y} into yall;
        read all var {_grp} into grp;
        close work._d;

        levels = unique(grp);
        k = ncol(levels);
        _grp = j(k, 1, "            ");
        first_stage_f = j(k, 1, .);
        do i = 1 to k;
            idx = loc(grp = levels[i]);
            X = j(ncol(idx), 1, 1) || Z[idx, ];
            yk = yall[idx];
            n = nrow(X);
            XtXi = inv(X` * X);
            beta = XtXi * X` * yk;
            g = X # (yk - X * beta);
            S = g` * g;
            do h = 1 to &hac_bandwidth - 1;
                if h < n then do;
                    C = g[(h + 1):n, ]` * g[1:(n - h), ];
                    S = S + (1 - h / &hac_bandwidth) * (C + C`);
                end;
            end;
            V = XtXi * S * XtXi;
            r = 2:ncol(X);
            wald = beta[r]` * inv(V[r, r]) * beta[r];
            first_stage_f[i] = wald / ncol(r);
            _grp[i] = levels[i];
        end;

        create &out var {_grp first_stage_f};
        append;
        close &out;
    quit;
%mend;

/* One regression per _grp on work._d, written to &out as one row per term
   with coef, se, t, p_value, n_obs and r2.
     test=  variables to test jointly; the F statistic goes to &out._test
     pred=  name of the fitted values, added to work._d */
%macro _regress(y, xs, pooled, out, test=, pred=);
    %local namecol q;
    %let q = %sysfunc(countw(&test, %str( )));

    ods exclude all;
    %if &pooled %then %do;
        %let namecol = Parameter;
        ods output ParameterEstimates=work._raw FitStatistics=work._rawfit
            %if &q %then Contrasts=work._rawtest;;
        proc surveyreg data=work._d;
            by _grp;
            class symbol;
            cluster trade_date;
            model &y = &xs symbol / solution;
            %if &q %then %do;
                contrast "joint" %sysfunc(tranwrd(%sysfunc(compbl(&test)), %str( ), %str( 1, ))) 1;
            %end;
            %if %length(&pred) %then %do;
                output out=work._dp p=&pred;
            %end;
        run;
    %end;
    %else %do;
        %let namecol = Variable;
        ods output ParameterEstimates=work._raw FitSummary=work._rawfit;
        proc autoreg data=work._d;
            by _grp;
            model &y = &xs / covest=hac(kernel=bartlett, bandwidth=&hac_bandwidth);
            %if %length(&pred) %then %do;
                output out=work._dp pm=&pred;
            %end;
        run;
    %end;
    ods exclude none;

    data work._pe;
        length term $32;
        set work._raw;
        term = lowcase(strip(&namecol));
        coef = Estimate;
        se = StdErr;
        t = tValue;
        p_value = Probt;
        keep _grp term coef se t p_value;
    run;

    /* R-squared: SURVEYREG labels it "R-Square"; AUTOREG prints two columns
       of fit statistics and calls it "Total R-Square". */
    data work._r2(keep=_grp r2);
        set work._rawfit;
        %if &pooled %then %do;
            if upcase(strip(Label1)) = "R-SQUARE" then do; r2 = nValue1; output; end;
        %end;
        %else %do;
            if index(upcase(Label1), "TOTAL R-SQUARE") then do; r2 = nValue1; output; end;
            else if index(upcase(Label2), "TOTAL R-SQUARE") then do; r2 = nValue2; output; end;
        %end;
    run;

    proc sql;
        create table work._n as
        select _grp, count(*) as n_obs from work._d group by _grp;
    quit;

    data &out;
        merge work._pe(in=_a) work._n work._r2;
        by _grp;
        if _a;
    run;

    /* Joint test of the instruments, as an F statistic: Wald chi-square over
       the number of restrictions, which is what statsmodels' f_test reports.
       Pooled: SURVEYREG's CONTRAST, with the clustered covariance. Per pair:
       computed in IML, because AUTOREG's TEST output table is not reliably
       named across SAS/ETS releases. */
    %if &q and &pooled %then %do;
        data &out._test(keep=_grp first_stage_f);
            set work._rawtest;
            first_stage_f = FValue;
        run;
    %end;
    %else %if &q %then %do;
        %_hac_wald(&y, &xs, &out._test)
    %end;

    %if %length(&pred) %then %do;
        data work._d;
            set work._dp;
        run;
    %end;
%mend;

%macro _append(model, est, terms, cov_type, r2=1);
    data work._new;
        if 0 then set work.results;
        set &est;
        where indexw(upcase("&terms"), upcase(strip(term))) > 0;
        model = "&model";
        sample = _grp;
        cov_type = "&cov_type";
        %if not &r2 %then r2 = .;;
        keep model sample term coef se t p_value n_obs r2 cov_type first_stage_f;
    run;
    proc append base=work.results data=work._new;
    run;
%mend;

%macro ols(model, y, xs);
    %_sample(&y &xs, 1)
    %_regress(&y, &xs, 1, work._est)
    %_append(&model, work._est, &xs, cluster(date))

    %_sample(&y &xs, 0)
    %_regress(&y, &xs, 0, work._est)
    %_append(&model, work._est, &xs, HAC(&hac_lags))
%mend;

%macro iv1(model, y, endog, instr, pooled);
    %local k i e hats;
    %let k = %sysfunc(countw(&endog, %str( )));
    %let hats = ;
    %_sample(&y &endog &instr, &pooled)

    /* First stages: each endogenous regressor on the instruments (plus pair
       effects when pooled); keep the fitted values and the F statistic. */
    %do i = 1 %to &k;
        %let e = %scan(&endog, &i, %str( ));
        %let hats = &hats hat_&e;
        %_regress(&e, &instr, &pooled, work._first, test=&instr, pred=hat_&e)
        data work._fs&i;
            length term $32;
            set work._first_test;
            term = "&e";
        run;
    %end;

    /* Second stage: its coefficients are the 2SLS estimates. */
    %_regress(&y, &hats, &pooled, work._second)

    proc transpose data=work._second(where=(term =: "hat_")) out=work._b(drop=_name_) prefix=b_;
        by _grp;
        id term;
        var coef;
    run;

    /* Fitted part plus structural residual. Regressing it on the fitted values
       returns the same coefficients, with standard errors built from the
       structural residuals. */
    data work._d;
        merge work._d work._b;
        by _grp;
        y_star = &y
        %do i = 1 %to &k;
            %let e = %scan(&endog, &i, %str( ));
            - (&e - hat_&e) * b_hat_&e
        %end;
        ;
    run;

    %_regress(y_star, &hats, &pooled, work._est)

    data work._est;
        set work._est;
        where term =: "hat_";
        term = substr(term, 5);
    run;

    data work._fs;
        set work._fs1-work._fs&k;
    run;

    proc sort data=work._est; by _grp term; run;
    proc sort data=work._fs; by _grp term; run;

    data work._est;
        merge work._est(in=_a) work._fs;
        by _grp term;
        if _a;
    run;

    %if &pooled %then %_append(&model, work._est, &endog, cluster(date), r2=0);
    %else %_append(&model, work._est, &endog, HAC(&hac_lags), r2=0);
%mend;

%macro iv(model, y, endog, instr);
    %iv1(&model, &y, &endog, &instr, 1)
    %iv1(&model, &y, &endog, &instr, 0)
%mend;

/* The models in analysis.SPECS and analysis.IV_SPECS. */
%ols(levels,             lambda,          realized_vol_pct volume_busd)
%ols(logs,               log_lambda,      log_realized_vol log_volume)
%ols(high_vol_day,       lambda,          high_vol)
%ols(logs_lagged_vol,    log_lambda,      log_realized_vol_lag log_volume)
%ols(split_sample,       log_lambda_even, log_realized_vol_odd log_volume_odd)
%ols(split_sample_resid, log_lambda_even, log_resid_vol_odd log_volume_odd)
%iv(iv_lagged, log_lambda, log_realized_vol log_volume, log_realized_vol_lag log_volume_lag)

/* Fail loudly instead of appending tables left over from an earlier step. */
%macro _clear(names);
    proc datasets lib=work nolist nowarn;
        delete &names;
    quit;
%mend;

/* Tidy a ParameterEstimates table from PROC PANEL or PROC SYSLIN into the
   layout _append expects. The column holding the coefficient names is not
   called the same in every procedure, so it is found by its contents: the
   character column whose value is one of &terms. */
%macro _tidy(raw, terms, out);
    data work._tidy;
        length _grp $12 term $32;
        set &raw;
        array _c{*} _character_;
        term = "";
        do _i = 1 to dim(_c);
            _v = upcase(vname(_c{_i}));
            if missing(term) and _v not in ("_GRP", "TERM")
               and indexw(upcase("&terms"), upcase(strip(_c{_i}))) > 0
                then term = lowcase(strip(_c{_i}));
        end;
        if not missing(term);
        if missing(_grp) then _grp = "pooled";
        coef = Estimate;
        se = StdErr;
        t = tValue;
        p_value = Probt;
        keep _grp term coef se t p_value;
    run;

    proc sql;
        create table &out as
        select e.*, n.n_obs, . as r2, . as first_stage_f
        from work._tidy as e
        left join (select _grp, count(*) as n_obs from work._d group by _grp) as n
          on e._grp = n._grp;
    quit;
%mend;

%macro _require(ds);
    %if not %sysfunc(exist(&ds)) %then %do;
        %put ERROR: &ds was not created; see the log above. Stopping.;
        %abort cancel;
    %end;
%mend;

/* Cross-check 1: PROC PANEL with one-way (pair) fixed effects. Same slopes as
   the pooled "logs" model; classical standard errors. */
%_clear(_raw _est)
%_sample(log_lambda log_realized_vol log_volume, 1)
ods exclude all;
ods output ParameterEstimates=work._raw;
proc panel data=work._d;
    id symbol trade_date;
    model log_lambda = log_realized_vol log_volume / fixone;
run;
ods exclude none;

%_require(work._raw)
%_tidy(work._raw, log_realized_vol log_volume, work._est)
%_require(work._est)
%_append(logs_panel_fixone, work._est, log_realized_vol log_volume, classical)

/* Cross-check 2: PROC SYSLIN two-stage least squares, per pair. Same slopes
   as iv_lagged; classical standard errors. */
%_clear(_raw _est)
%_sample(log_lambda log_realized_vol log_volume log_realized_vol_lag log_volume_lag, 0)
ods exclude all;
ods output ParameterEstimates=work._raw;
proc syslin data=work._d 2sls;
    by _grp;
    endogenous log_realized_vol log_volume;
    instruments log_realized_vol_lag log_volume_lag;
    model log_lambda = log_realized_vol log_volume;
run;
ods exclude none;

%_require(work._raw)
%_tidy(work._raw, log_realized_vol log_volume, work._est)
%_require(work._est)
%_append(iv_lagged_syslin, work._est, log_realized_vol log_volume, classical)

data micro.sas_results;
    set work.results;
    format coef se t p_value r2 first_stage_f best16.;
run;

proc export data=micro.sas_results
            outfile="&out/sas_results.csv" dbms=csv replace;
run;

title "Second-stage slopes from SAS";
proc print data=micro.sas_results noobs label;
    var model sample term coef se t n_obs first_stage_f;
    format coef se t 10.4 first_stage_f 8.2;
run;
title;
