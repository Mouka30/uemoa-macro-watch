# UEMOA Macro Watch

This project builds a macroeconomic monitoring system for the UEMOA region using Python.
It is designed for economic journalism, strategic monitoring, and data-driven analysis.

## Scope
- Region: UEMOA (Benin, Burkina Faso, Côte d’Ivoire, Guinea-Bissau, Mali, Niger, Senegal, Togo)
- Level: Macro-economic
- Indicators (v0.1):
  - Inflation (by country)
  - BCEAO policy rate
  - CFA franc exchange rate

## Data Sources
- BCEAO (Central Bank of West African States)
- National statistical offices (INS/ANSD – upcoming)

## Project Structure
src/ # Python scripts (scraping, parsing)
data/raw/ # Raw HTML files (not versioned)
data/processed/ # Processed CSV files (not versioned)
docs/ # Methodology and monitoring framework

## Current Pipeline
1. Fetch BCEAO press releases
2. Parse and identify monetary policy decisions
3. Extract policy rates into structured datasets (next step)

## Purpose
- Learn web scraping and data structuring with Python
- Build a reusable economic monitoring engine
- Support data-driven economic journalism on West Africa
