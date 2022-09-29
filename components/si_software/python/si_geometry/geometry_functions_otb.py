#!/usr/bin/env python3
# -*- coding: utf-8 -*-


""" 
    geometry functions with otb dependencies
"""

from si_geometry.geometry_functions import *
import otbApplication as otb


def band_math(input_rasters, output_raster, expression, ram=None, out_type=None):
    """ram in MB"""
    bandMathApp = otb.Registry.CreateApplication("BandMath")
    bandMathApp.SetParameterString("exp", expression)
    for input_raster in input_rasters:
        bandMathApp.AddParameterStringList("il", input_raster)
    bandMathApp.SetParameterString("out", output_raster)
    if ram is not None:
        bandMathApp.SetParameterString("ram", str(ram))
    if out_type is not None:
        bandMathApp.SetParameterOutputImagePixelType("out", out_type)
    bandMathApp.ExecuteAndWriteOutput()
    bandMathApp = None
    

