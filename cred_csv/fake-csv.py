from faker import Faker
import random
import pandas as pd

Faker.seed(0)
random.seed(0)
fake = Faker("en_US") 
fixed_digits = 6
concatid = 'ID'
ID,Name,Date,Amount= [[] for k in range(0,4)]

for row in range(0,10000000):
    ID.append(str(fake.unique.random_int(min=1, max=99999999)))
    Name.append(fake.name())
    Date.append(fake.date())
    Amount.append(random.randint(1111,9999999999))

d = {"ID":ID, "Name":Name, "Date":Date, "Amount":Amount}
df = pd.DataFrame(d)
print(df.head())
df.to_csv('fake-csv.csv', index=False)