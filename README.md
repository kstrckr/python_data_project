# Iowa State Liquor Sales Exploration

**How do annual sales compare county to county and does that relate to ml purchased per dollar? aka, do counties with higher sales buy more expensive liquor?**

To view this report:
1. Install Anaconda -  [https://www.anaconda.com/download/](https://www.anaconda.com/download/)
2. Clone this repo
3. In the project's root folder **run** `setup.py`
4. Download the liquor sales source data either [from my dropbox](https://www.dropbox.com/s/db64nw579etnx72/iowa-liquor-sales.zip?dl=0) 
Or if you have a kaggle account, [here](https://www.kaggle.com/residentmario/iowa-liquor-sales)
5. Unzip the source data into project's `root/input/iowa-liquor-sales/`
6. Run `seed_data.py`
    **This step can take a while, it will create the database, create the tables, then parse the data and insert it into several tables** - 2.2 million rows are being parsed and inserted. A mid-range 5 year old desktop takes ~150 seconds.
7. After seeing the `database seeding took X seconds` launch Anaconda, either Navigator or Prompt. 
    **A**. If using prompt, navigate to the project's root directory, run `jupyter notebook Iowa_liquor_data_vis.ipynb`
    **B**. If using navigator, launch Jupyter Notebook from the main menu and manually navigate to the project's root, click on `Iowa_liquor_data_vis.ipynb` to open the notebook
8. In the jupyter notebook's menu bar, select `Cell` and `Run All`

sneak-peek at graphed data:

![map of annual sales](https://www.kurtstrecker.com/py_data_imgs/annual_sales_map.png)

![map of ml/dollar spent](https://www.kurtstrecker.com/py_data_imgs/vol_map.png)

![scatter plot of sales vs vol/dollar vs population](https://www.kurtstrecker.com/py_data_imgs/sales_vs_vol_vs_pop.png)