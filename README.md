# usgs-daily-streamflow-percentiles



This repository was created to replace the functionality of the 1, 7, 14, and 28 day streamflow percentile maps that were previously hosted on the USGS WaterWatch web page. The WaterWatch web pages were removed in February 2026 as part of a USGS modernization effort. More information on the modernization effort can be found [here](https://waterdata.usgs.gov/blog/wdfn-stats-delivery/).  

Streamflow data is downloaded and processed utilizing USGS Python packages `dataretrieval` and `hyswap`. [This USGS tutorial](https://doi-usgs.github.io/hyswap/examples/site_conditions_examples.html) was also used as a template for map creation. The general workflow is described below. 

* All available daily streamflow data is downloaded locally using USGS APIs
  * Data is saved locally and future calls will append to and/or load local data
* Daily streamflow data is used to calculate specified percentiles on approved data with n-day rolling averages 
* An estimated percentile for the most recent daily value (provisional) is interpolated from the calculated percentiles
  * Note that the most recent values for each gage may not be for the same day. Daily values can have a delay in reporting.

* Data are displayed on interactive maps created with `folium` and saved as `.html` files    
  * Scroll over points to see additional information, including the date of the most recent daily data 




### Webpage

The maps are currently viewable on the GitHub Page associated with this repository at this link [https://colby-thrash.github.io/usgs-daily-streamflow-percentiles/](https://colby-thrash.github.io/usgs-daily-streamflow-percentiles/). The markdown file `index.md` is what is displayed at the link. 



## Getting Started

### Setup 

The repository was created using `uv` and setup instructions below utilize `uv` to download dependencies from the `pyproject.toml` file and run the project locally. 

Learn more about `uv` [here]([Installation | uv](https://docs.astral.sh/uv/getting-started/installation/)). 

1. Clone the repo

   ```
   git clone https://github.com/colby-thrash/usgs-daily-streamflow-percentiles.git
   ```

2. Navigate to the project

   ```
   cd usgs-daily-streamflow-percentiles
   ```

3. Download dependencies

   ```
   uv sync
   ```



### **API Key**

Note that calls to the USGS APIs will be limited without an API key. The rate limit *with* a key is 1,000 requests per hour.

Get your API key here:  https://api.waterdata.usgs.gov/signup/

Once you get it, save the API key locally in a text file named `usgs_api_key.txt` in the main project directory `\usgs-daily-streamflow-percentiles`.



### Usage

Run the code with `uv`:

```
uv run src/main.py
```





