import os
import pandas as pd


def save_pdf(response, filepath: str):
    # divide into folders
    folders = filepath.split('/')
    for i in range(1, len(folders)):
        folder = '/'.join(folders[:i])
        if not os.path.exists(folder):
            os.mkdir(folder)
    # save PDF content
    with open(filepath, 'wb') as f:
        f.write(response.content)

def save_csv(df, filepath: str):
    # divide into folders
    folders = filepath.split('/')
    for i in range(1, len(folders)):
        folder = '/'.join(folders[:i])
        if not os.path.exists(folder):
            os.mkdir(folder)
    # save CSV content
    df.to_csv(filepath, index=False)

months = [
    'January',
    'February',
    'March',
    'April',
    'May',
    'June',
    'July',
    'August',
    'September',
    'October',
    'November',
    'December',
]
