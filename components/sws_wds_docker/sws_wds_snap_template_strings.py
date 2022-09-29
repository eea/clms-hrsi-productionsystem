# SNAP graph templates for SWS/WDS Product generation

xml_graph_init = """
<graph id="Graph">
  <version>1.0</version>
</graph>
"""

xml_graph_read = """
<node id="Read(%(readId)s)">
  <operator>Read</operator>
  <sources/>
  <parameters class="com.bc.ceres.binding.dom.XppDomElement">
    <file>%(readFile)s</file>
    <formatName>%(readFormat)s</formatName>
  </parameters>
</node>
"""

#    <orbitType>Sentinel Precise (Auto Download)</orbitType>
#    <orbitType>Sentinel Restituted (Auto Download)</orbitType>
xml_graph_orbit = """
<node id="Apply-Orbit-File(%(readId)s)">
  <operator>Apply-Orbit-File</operator>
  <sources>
    <sourceProduct refid="Read(%(readId)s)"/>
  </sources>
  <parameters class="com.bc.ceres.binding.dom.XppDomElement">
    <orbitType>Sentinel %(orbitType)s (Auto Download)</orbitType>
    <polyDegree>%(orbitPolyDegree)d</polyDegree>
    <continueOnFail>false</continueOnFail>
  </parameters>
</node>
"""

xml_graph_bordernoise = """
<node id="Remove-GRD-Border-Noise(%(readId)s)">
  <operator>Remove-GRD-Border-Noise</operator>
  <sources>
     <sourceProduct refid="%(rmBorderNoiseSourceProduct)s(%(readId)s)"/>
  </sources>
  <parameters class="com.bc.ceres.binding.dom.XppDomElement">
    <selectedPolarisations>VH,VV</selectedPolarisations>
    <borderLimit>500</borderLimit>
    <trimThreshold>0.5</trimThreshold>
  </parameters>
</node>
"""

xml_graph_thermalnoiseremoval = """
 <node id="ThermalNoiseRemoval(%(readId)s)">
    <operator>ThermalNoiseRemoval</operator>
    <sources>
    <sourceProduct refid="Remove-GRD-Border-Noise(%(readId)s)"/>
    </sources>
    <parameters class="com.bc.ceres.binding.dom.XppDomElement">
      <selectedPolarisations/>
      <removeThermalNoise>true</removeThermalNoise>
      <reIntroduceThermalNoise>false</reIntroduceThermalNoise>
    </parameters>
  </node>
"""

xml_graph_calibration = """
<node id="Calibration(%(readId)s)">
  <operator>Calibration</operator>
  <sources>
    <sourceProduct refid="ThermalNoiseRemoval(%(readId)s)"/>
  </sources>
  <parameters class="com.bc.ceres.binding.dom.XppDomElement">
    <sourceBands/>
    <auxFile>Product Auxiliary File</auxFile>
    <externalAuxFile/>
    <outputImageInComplex>false</outputImageInComplex>
    <outputImageScaleInDb>false</outputImageScaleInDb>
    <createGammaBand>false</createGammaBand>
    <createBetaBand>false</createBetaBand>
    <selectedPolarisations>VH,VV</selectedPolarisations>
    <outputSigmaBand>false</outputSigmaBand>
    <outputGammaBand>false</outputGammaBand>
    <outputBetaBand>true</outputBetaBand>
  </parameters>
</node>
"""

xml_graph_assembly = """
<node id="SliceAssembly(%(assemblyId)s)">
  <operator>SliceAssembly</operator>
  <sources>
    %(xml_graph_assembly_source)s
  </sources>
  <parameters class="com.bc.ceres.binding.dom.XppDomElement">
    <selectedPolarisations/>
  </parameters>
</node>
"""

xml_graph_assembly_source_line = """
    <sourceProduct.%(Anum)d refid="Calibration(%(readId)s)"/>
"""

xml_graph_multilook = """
<node id="Multilook(%(assemblyId)s)">
  <operator>Multilook</operator>
  <sources>
    <sourceProduct refid="%(multilookSource)s"/>
  </sources>
  <parameters class="com.bc.ceres.binding.dom.XppDomElement">
    <sourceBands/>
    <nRgLooks>6</nRgLooks>
    <nAzLooks>6</nAzLooks>
    <outputIntensity>true</outputIntensity>
    <grSquarePixel>true</grSquarePixel>
    </parameters>
</node>
"""

xml_graph_terrainflattening = """
  <node id="Terrain-Flattening(%(assemblyId)s">
    <operator>Terrain-Flattening</operator>
    <sources>
      <sourceProduct refid="Multilook(%(assemblyId)s)"/>
    </sources>
    <parameters class="com.bc.ceres.binding.dom.XppDomElement">
      <sourceBands/>
      <demName>External DEM</demName>
      <demResamplingMethod>NEAREST_NEIGHBOUR</demResamplingMethod>
      <externalDEMFile>XXX.tif</externalDEMFile>
      <externalDEMNoDataValue>-32767</externalDEMNoDataValue>
      <externalDEMApplyEGM>false</externalDEMApplyEGM>
      <outputSimulatedImage>false</outputSimulatedImage>
      <additionalOverlap>0.1</additionalOverlap>
      <oversamplingMultiple>1.0</oversamplingMultiple>
    </parameters>
  </node>
"""

xml_graph_subset = """
  <node id="Subset(%(assemblyId)s)">
    <operator>Subset</operator>
    <sources>
      <sourceProduct refid="SliceAssembly(%(assemblyId)s)"/>
    </sources>
    <parameters class="com.bc.ceres.binding.dom.XppDomElement">
    <sourceBands/>
      <region>%(subset_region)s</region>
      <referenceBand/>
      <subSamplingX>1</subSamplingX>
      <subSamplingY>1</subSamplingY>
      <fullSwath>false</fullSwath>
      <tiePointGridNames/>
      <copyMetadata>true</copyMetadata>
    </parameters>
  </node>
"""

#      <geoRegion>%(geoRegion)s</geoRegion>
xml_graph_terraincorrection = """
<node id="Terrain-Correction(%(terraincorrectionId)s)">
  <operator>Terrain-Correction</operator>
  <sources>
    <sourceProduct refid="%(terraincorrection_source)s(%(assemblyId)s)"/>
  </sources>
  <parameters class="com.bc.ceres.binding.dom.XppDomElement">
    <sourceBands/>
    <demName>External DEM</demName>
    <externalDEMFile>%(externalDEMFile)s</externalDEMFile>
    <externalDEMNoDataValue>-32767</externalDEMNoDataValue>
    <externalDEMApplyEGM>false</externalDEMApplyEGM>
    <demResamplingMethod>NEAREST_NEIGHBOUR</demResamplingMethod>
    <imgResamplingMethod>%(IMGRESAMPLINGMETHOD)s</imgResamplingMethod>
    <pixelSpacingInMeter>%(PIXELSIZE)d</pixelSpacingInMeter>
    <pixelSpacingInDegree>0.0</pixelSpacingInDegree>
    <mapProjection>%(PROJCS)s</mapProjection>
    <alignToStandardGrid>true</alignToStandardGrid>
    <standardGridOriginX>%(STANDARDGRIDORIGIN)f</standardGridOriginX>
    <standardGridOriginY>%(STANDARDGRIDORIGIN)f</standardGridOriginY>
    <nodataValueAtSea>true</nodataValueAtSea>
    <saveDEM>false</saveDEM>
    <saveLatLon>false</saveLatLon>
    <saveIncidenceAngleFromEllipsoid>false</saveIncidenceAngleFromEllipsoid>
    <saveLocalIncidenceAngle>%(locincangl)s</saveLocalIncidenceAngle>
    <saveProjectedLocalIncidenceAngle>false</saveProjectedLocalIncidenceAngle>
    <saveSelectedSourceBand>%(saveSelectedSourceBand)s</saveSelectedSourceBand>
    <outputComplex>false</outputComplex>
    <applyRadiometricNormalization>false</applyRadiometricNormalization>
    <saveSigmaNought>false</saveSigmaNought>
    <saveGammaNought>false</saveGammaNought>
    <saveBetaNought>false</saveBetaNought>
    <incidenceAngleForSigma0>Use projected local incidence angle from DEM</incidenceAngleForSigma0>
    <incidenceAngleForGamma0>Use projected local incidence angle from DEM</incidenceAngleForGamma0>
    <auxFile>Latest Auxiliary File</auxFile>
    <externalAuxFile/>
  </parameters>
</node>
"""

xml_graph_write = """
<node id="Write(%(writeId)s)">
  <operator>Write</operator>
  <sources>
    <sourceProduct refid="%(writeSource)s"/>
  </sources>
  <parameters class="com.bc.ceres.binding.dom.XppDomElement">
    <file>%(writeFile)s</file>
    <formatName>%(writeFormat)s</formatName>
  </parameters>
</node>
"""

xml_graph_reproject = """
<node id="Reproject(%(reprojectId)s)">
  <operator>Reproject</operator>
  <sources>
    <sourceProduct refid="%(reprojectSource)s"/>
  </sources>
  <parameters class="com.bc.ceres.binding.dom.XppDomElement">
    <wktFile/>
      <crs>%(PROJCS)s</crs>
      <resampling>Nearest</resampling>
      <referencePixelX>0</referencePixelX>
      <referencePixelY>0</referencePixelY>
      <easting>%(EASTING)d</easting>
      <northing>%(NORTHING)d</northing>
      <orientation>0.0</orientation>
      <pixelSizeX>%(PIXELSIZE)d</pixelSizeX>
      <pixelSizeY>%(PIXELSIZE)d</pixelSizeY>
      <width>%(WIDTH)d</width>
      <height>%(HEIGHT)d</height>
      <tileSizeX/>
      <tileSizeY/>
      <orthorectify>false</orthorectify>
      <elevationModelName/>
      <noDataValue>0</noDataValue>
      <includeTiePointGrids>false</includeTiePointGrids>
      <addDeltaBands>false</addDeltaBands>
    </parameters>
  </node>
"""

xml_graph_setnodatavalue = """
  <node id="SetNoDataValue(%(reprojectId)s)">
    <operator>SetNoDataValue</operator>
    <sources>
      <sourceProduct refid="Reproject(%(reprojectId)s)"/>
    </sources>
    <parameters class="com.bc.ceres.binding.dom.XppDomElement">
      <noDataValueUsed>true</noDataValueUsed>
      <noDataValue>0.0</noDataValue>
    </parameters>
  </node>
"""

xml_graph_bandmath = """
  <node id="BandMaths(%(reprojectId)s)">
    <operator>BandMaths</operator>
    <sources>
      <sourceProduct refid="Reproject(%(reprojectId)s)"/>
      <sourceProduct.1 refid="Read(%(readId)s)"/>
    </sources>
    <parameters class="com.bc.ceres.binding.dom.XppDomElement">
      <targetBands>
        <targetBand>
          <name>wetSnow</name>
          <type>float32</type>
          <expression>%(bandmaths_expression)s</expression>
          <description/>
          <unit/>
          <noDataValue>0.0</noDataValue>
        </targetBand>
      </targetBands>
      <variables/>
    </parameters>
  </node>
"""

xml_graph_sarsim = """
<node id="SAR-Simulation(%(assemblyId)s)">
    <operator>SAR-Simulation</operator>
    <sources>
      <sourceProduct refid="%(sarsim_source)s(%(assemblyId)s)"/>
    </sources>
    <parameters class="com.bc.ceres.binding.dom.XppDomElement">
      <sourceBands/>
      <demName>External DEM</demName>
      <demResamplingMethod>NEAREST_NEIGHBOUR</demResamplingMethod>
      <externalDEMFile>%(sarsim_externalDEMFile)s</externalDEMFile>
      <externalDEMNoDataValue>-32767</externalDEMNoDataValue>
      <externalDEMApplyEGM>false</externalDEMApplyEGM>
      <saveLayoverShadowMask>true</saveLayoverShadowMask>
    </parameters>
  </node>
"""
