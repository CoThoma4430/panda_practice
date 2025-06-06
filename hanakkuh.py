import pandas as pd

path = "./5784/noahs-customers.csv"

df = pd.read_csv(path)

df.info()
print(df['phone'])
print(df.head())

name = 'Cody Thoma'

print(name.split()[-1])


def name_to_numb(name):

    t9_name  = ''

    t9 = {
        '2': 'abc',
        '3': 'def',
        '4': 'ghi',
        '5': 'jkl',
        '6': 'mno',
        '7': 'pqrs',
        '8': 'tuv',
        '9': 'wxyz',
    }

    for letter in name.split()[-1].lower():
        for numbers, letters in t9.items():
            if letter in letters:
                t9_name += numbers
                break
    
    return t9_name

print(name_to_numb(name))
