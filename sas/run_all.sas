/*---------------------------------------------------------------------------
  run_all.sas - the whole SAS replication, in order.

  In SAS Studio: upload the sas/ folder to ~/microstructure/sas and the
  Databricks exports to ~/microstructure/exports, open this file and run it.
  Results land in ~/microstructure/sas_output; download sas_results.csv and
  sas_lambda_daily.csv from there for the parity table
  (python -m microstructure.parity).
---------------------------------------------------------------------------*/

%let sasdir = %sysget(HOME)/microstructure/sas;

%include "&sasdir/00_setup.sas";
%include "&sasdir/01_daily_lambda.sas";
%include "&sasdir/02_volatility.sas";
