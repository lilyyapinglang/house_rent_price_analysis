
# Shanghai Rental Apartment Market Research and price prediction

##  Motivation for the project

As young professional living in metropolitan like Shanghai, housing cost could take a large part of monthly income, usually 1/3 or 1/4. Finding a rental apartment which is of good size, location, close to metro, easy commute to office , and for a good value of money can be a daunting task for many.  

I would like to do some research on Shanghai's rental apartment market to prepare myself for finding next apartment, and also find some interesting insights to share to others who may be interested. Research range such as which subarea is the good value of money, which area offers more houses, what should be a reasonable price given its location, number of rooms, etc. I hope next time when I'm visiting houses I can roughly know if the price offered by agent is a reasonable one.

## Data used 

1. Shanghai apartment rental listing data scraped from lianjia.com (31071 rows × 34 columns)
2. Shanghai metro data (584 rows × 5 columns)
3. Shanghai compounds & its built year (10371 rows x 3columns)
4. Shanghai resident population and density by district

All stored in `.\db\house_rent_lianjia.db`.  

## Steps taken
1. Data exploration and cleaning 
    - drop unused columns
    - correct data types
    - deal with missing values 
    - remove numerical outliers
    - one hot encoding selected categorical variables
2. Perform statistical analysis and visualization to answer some insight questions   
3. Preliminary model selection
    - train and evaluate several base regression model to do preliminary selection : Linear Regression, Random Forest, Gradient Boosting , SVM, XGBoost, Neural Network.
4. Hyperparamter tuning on best performing models from above step
    - GridSearch on RandomForest model
    - GridSearch on XGBoost model 
5. Model stacking 
    - Use RandomForest(best parameter) and XGboost(best parameter) as base models, experiment using  Linear Regression, Ridge Regression, Gradient Boosting as meta-learner, benchmarking the performance
    - Choose Linear Regression as meta-learner as it performs the best 
6. Feature Engineering
    - Check the feature importance of both base models: RandomForest, XGBoost
    - Further identify and remove outliers of the most importance feature
    - Add a new feature `property_age`

## Models Used and results 

Following above steps , and through trial and error, the project obtained below results:

| Step |       Technique        |              Model               | Mean Squared Error | Root Mean Squared Error | Mean Absolute Error | R-Squared |                      Comment                      |
|------|-----------------------|---------------------------------|--------------------|------------------------|---------------------|-----------|--------------------------------------------------|
|  1   | Preliminary Model Selection |      Linear Regression          |   2.830890e+07    |      5320.610995       |    2282.561717      | 0.605319  |                                                  |
|  1   | Preliminary Model Selection | Random Forest                   |   1.422363e+07    |      3771.422470       |    1392.152518      | 0.801695  |    Selected for step     2                                          |
|  1   | Preliminary Model Selection | Gradient Boosting               |   1.827191e+07    |      4274.565994       |    1619.775862      | 0.745254  |                                                  |
|  1   | Preliminary Model Selection | Neural Network                  |   2.914896e+07    |      5398.977928       |    2363.423756      | 0.593607  |                                                  |
|  1   | Preliminary Model Selection | XGBoost                         |   1.419436e+07    |      3767.540589       |    1416.981084      | 0.802103  |    Selected for step     2                                            |
|  1   | Preliminary Model Selection | SVM                             |   6.756874e+07    |      8220.020547       |    3277.116254      | 0.057961  |                                                  |
|  2   | Hyperparameter Tuning | Random Forest (Fine-tuned)     |   1.431105e+07    |      3782.995012       |    1389.606300      | 0.800476  |    Selected as base model for step 3                                              |
|  2   | Hyperparameter Tuning | XGBoost (Fine-tuned)           |   1.418118e+07    |      3765.790345       |    1422.060618      | 0.802287  |    Selected as base model for step 3                                            |
|  3   | Model Stacking       | Linear Regression (Meta Model)  |   1.188957e+07    |      3448.124931       |    1380.428049      | 0.811552  |    Selected as base model for step 4                                          |
|  3   | Model Stacking       | Ridge Regression (Meta Model)   |   1.188955e+07    |      3448.122744       |    1380.420325      | 0.811552  |                                                  |
|  3   | Model Stacking       | Gradient Boosting (Meta Model)  |   1.558438e+07    |      3947.705413       |    1414.866954      | 0.752990  |                                                  |
|  4   | Feature Engineering   | Removed area > 400             |   9.880533e+06    |      3143.331524       |    1361.709242      | 0.846688  | Outliers removal based on feature area_sqm > 400          |
|  5   | Feature Enginnering   | Added feature property_age     |   5.976616e+06    |      2444.711837       |    1143.796888      | 0.878396  | selected as final model                           |

Overall, the entire process of selecting models, fine tuning, model stacking, feature enginnering demonstrated the importance of selecting appropriate models, optimizing parameters, and incorporating feature engineering techniques to achieve accurate predictions of house prices.

## Libraries used 
- Python 3.11.3 
- Refer to `.\requirement.txt`

## Usage

1. Clone the repository: git clone https://github.com/username/rental-price-prediction.git
2. Navigate into the cloned repository
3. Install the necessary libraries: pip install -r requirements.txt
4. Go to `house_rental_shanghai.ipynb` and run the cells.

Note: You will need `Python 3.x` and `pip` installed on your machine to run this project.

## Files in the repository 

- collect_data 
    - `get_all_urls_rent.py`, scraping script to get all the urls of rental apartment in Lianjia.com
    - `get_sh_metro_coordiates.py`, fetch metro stations and coordiantes info from map API, save the results in .db 
    - `spider_rent_lianjia_final.py`, scraping script to script all apartment urls for detailed apartment attributes , save the results in .db 
- db 
    - `house_rent_lianjia.db`, db file that contains all the data needed in jyputer notebook 
- `house_rental_shanghai.ipynb`, contains end-to-end data analysis and model building code  

## License
The code in this project is licensed under the MIT license.
