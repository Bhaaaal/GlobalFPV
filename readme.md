# Data

- Country boundaries and boundary lakes were derived from the Natural Earth dataset.
- Hourly ice thickness, extreme precipitation, wind speed, surface solar radiation, and monthly 2-m temperature data for 2024 were derived from the ERA5-Land reanalysis dataset at 0.1° spatial resolution.
- Population data were derived from the GPWv4 dataset for 2020 at 2.5′ spatial resolution. Population centres were defined from GPWv4 grids with total population ≥ 1,000 and population density ≥ 400 people km⁻².
- Thermal infrared FPV images were derived from Landsat 8/9 TIR data acquired in 2023.
- Monthly lake-surface temperature data were derived from the GLAST dataset.
- Lake boundaries in GLAST were based on HydroLAKES and refined using the Global Surface Water Occurrence dataset.
- Monthly environmental variables were derived from TerraClimate for 2020 and 2023 at 0.5° spatial resolution.
- LCOE data were derived from IRENA’s 2024 release.
- Electricity demand and monthly hrdropower generation data were derived from Ember Monthly Electricity Data.
- Residential and commercial electricity prices were derived from GlobalPetrolPrices using country-level averages over 2023–2025.

Remote sensing and vector datasets are too large to be included in this repository. Please download them from the relevant data sources.

# Code

* 'InitFishnet.py': This script prepares ERA5-Land fishnet grids and spatial masks for further computation.
* 'SuitableArea.py': This script calculates the suitable area for FPV installation.Need to run codes in './CalSuitableArea' folder first.
* 'ExtarctSSRD.py': This script extracts SSRD data from ERA5-Land.
* 'ThetaZenith.py': This script calculates the zenith angle of the sun and theta of PV panels.
* 'GlobalCountryOverlay.py' This script overlays the global country boundaries with the FPV fishnet grids.
* 'ElectricityGeneration.py' This script calculates the electricity generation from FPV and GPV.
* 'HydroSolarBeluco.py' This script calculates the Beluco hydro-solar-complementary index.
* 'FPVprofit.py' This script calculates the FPV commercial and residential profit.

# Result

* 'monthly_power_by_country.csv': This file provides the monthly electricity generation from FPV and GPV.
* 'BelucoResults.csv': This file provides the Beluco hydro-solar-complementary index.
* 'FPVprofit.csv': This file provides the FPV commercial and residential profit.
