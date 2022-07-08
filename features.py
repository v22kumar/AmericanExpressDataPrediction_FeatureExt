from _read_data import train_parquet, name_function
from main import fill_missing
# Single Features
train_parquet['customer_ID'].head(10)

# Multiple structure
train_parquet[['customer_ID', 'S_2', 'S_3']].head(10)

# List
delinquency_features = [col for col in train_parquet if col.startswith('D_')]

spend_features = [col for col in train_parquet if col.startswith('S_')]

payment_features = [col for col in train_parquet if col.startswith('P_')]

balance_features = [col for col in train_parquet if col.startswith('B_')]

risk_features = [col for col in train_parquet if col.startswith('R_')]
print("delinquency_features:", len(delinquency_features))
print("spend_features:", len(spend_features))
print("payment_features:", len(payment_features))
print("balance_features:", len(balance_features))
print("risk_features:", len(risk_features))

# Analyse
train_parquet[payment_features]

# missing values in payment features
# payment_ncount = count_missing(train_parquet[payment_features])
# print('Missing Values in Payment Features:',payment_ncount)

# fill missing values
# if payment_ncount > 0:
train_parquet[payment_features]= fill_missing(train_parquet[payment_features])
train_parquet[spend_features] = \
train_parquet[spend_features]= fill_missing(train_parquet[spend_features])

# convert date to numeric
train_parquet['S_2'] = train_parquet['S_2'].str.replace('-','').astype('int64')
train_parquet['S_2'].head(10)
train_parquet[risk_features] = \
train_parquet[risk_features]= fill_missing(train_parquet[risk_features])

# Balance features
train_parquet[balance_features] =\
train_parquet[balance_features]= fill_missing(train_parquet[balance_features])

train_parquet[delinquency_features] =\
train_parquet[delinquency_features]= fill_missing(train_parquet[delinquency_features])

categorical_features = ['B_30', 'B_38', 'D_114', 'D_116', 'D_117', 'D_120', 'D_126', 'D_63', 'D_64', 'D_66', 'D_68']

train_parquet[categorical_features]['B_38'].head(10)

# Save
train_parquet.to_parquet('./train_parquet/', name_function=name_function)
