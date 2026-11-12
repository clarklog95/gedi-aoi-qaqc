"""Compatibility entry point; use python run_gedi.py combine for configured projects."""
import runpy
if __name__=='__main__':runpy.run_module('gedi_aoi.combine',run_name='__main__')
