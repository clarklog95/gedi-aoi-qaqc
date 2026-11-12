"""Lightweight progress viewer. Closing it does not stop the Parquet worker."""
import pathlib,json,datetime,tkinter as tk,argparse
from tkinter import ttk
parser=argparse.ArgumentParser();parser.add_argument('--project-root',type=pathlib.Path,required=True);ROOT=parser.parse_args().project_root.resolve();OUT=ROOT/'data/combined';PROGRESS=OUT/'PROGRESS.json';CONTROL=OUT/'CONTROL.json'
app=tk.Tk();app.title('GEDI - Combined Parquet progress');app.geometry('750x420');app.minsize(650,400)
ttk.Label(app,text='Combining all GEDI products',font=('Segoe UI',16,'bold')).pack(anchor='w',padx=20,pady=(18,10))
phase=tk.StringVar(value='Waiting for the worker...');ttk.Label(app,textvariable=phase,wraplength=700).pack(anchor='w',padx=20)
bar=ttk.Progressbar(app,length=700,maximum=100);bar.pack(fill='x',padx=20,pady=15)
counts=tk.StringVar();ttk.Label(app,textvariable=counts,font=('Segoe UI',11)).pack(anchor='w',padx=20)
details=tk.StringVar();ttk.Label(app,textvariable=details,font=('Consolas',10),justify='left').pack(anchor='w',padx=20,pady=15)
hint=tk.StringVar(value='Closing this window leaves the combination running.');ttk.Label(app,textvariable=hint,wraplength=700).pack(side='bottom',pady=12)
button=ttk.Button(app,text='Pause after current batch');button.pack(side='bottom',pady=6)
def toggle():
 try:c=json.loads(CONTROL.read_text())
 except (OSError,ValueError):c={}
 c['paused']=not c.get('paused',False);temp=CONTROL.with_suffix('.tmp');temp.write_text(json.dumps(c));temp.replace(CONTROL)
 button.config(text='Resume combination' if c['paused'] else 'Pause after current batch')
 hint.set('The current batch/request will finish before pausing.' if c['paused'] else 'Closing this window leaves the combination running.')
button.config(command=toggle)
def tick():
 try:
  s=json.loads(PROGRESS.read_text());total=s.get('total_rows',0);pct=s.get('overall_percent',0);bar['value']=pct;phase.set(s.get('phase','Starting'))
  counts.set(f'{pct:.1f}% overall | Batch {s.get("batch",0)} of {s.get("batch_count",0)}')
  seconds=int(s.get('elapsed_seconds',0));hours,remainder=divmod(seconds,3600);minutes,seconds=divmod(remainder,60)
  details.set(f'Batch rows verified: {s.get("batch_rows_verified",0):,} / {total:,}\nFinal rows written:  {s.get("final_rows_written",0):,} / {total:,}\nFinal rows verified: {s.get("final_rows_verified",0):,} / {total:,}\n\nFinal file: {s.get("file_bytes",0)/1e9:.3f} GB | Staging: {s.get("staging_bytes",0)/1e9:.3f} GB\nElapsed: {hours:02}:{minutes:02}:{seconds:02} | Free disk: {s.get("free_bytes",0)/1e9:.1f} GB')
  if s.get('status')=='complete':button.config(state='disabled');hint.set('Complete: the single Parquet file passed all checks. Safe to close this window.')
  elif s.get('status')=='failed':button.config(state='disabled');hint.set('Worker stopped: '+s.get('error','See the log')+'. Verified batches are retained.')
  else:
   button.config(state='normal')
   hint.set('Closing this window leaves the combination running.')
   age=(datetime.datetime.now(datetime.timezone.utc)-datetime.datetime.fromisoformat(s['updated_utc'])).total_seconds()
   if age>30:hint.set('No recent heartbeat. Check the worker log before restarting.')
 except (OSError,ValueError,KeyError):pass
 app.after(1000,tick)
tick();app.mainloop()