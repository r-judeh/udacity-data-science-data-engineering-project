import pandas as pd
from sqlalchemy import create_engine


class ETLPipe:
    def __init__(self, database_filepath):
        self._df = None
        self.engine = create_engine('sqlite:///' + database_filepath)

    @property
    def data(self):
        return self._df

    def load_data_csv(self, messages_filepath, categories_filepath, clean_data=False):
        """ loads data from csv files and merges them into a DataFrame
        :param str messages_filepath: absolute path to the messages csv file
        :param str categories_filepath: absolute path to the categories csv file
        :param bool clean_data: whether to clean data after or loading or not (default False)
        :return: pd.DataFrame: pandas dataframe of merged messages and categories
        """

        messages = pd.read_csv(messages_filepath)
        categories = pd.read_csv(categories_filepath)

        self._df = messages.merge(categories, on='id', how='inner')

        if clean_data:
            self.clean_data(self._df)

        return self._df

    def load_data_db(self, table_name):
        self._df = pd.read_sql(f'SELECT * FROM {table_name}', self.engine)

        return self._df

    def save_data_db(self, df, table_name):
        """ saves the data into a database
        :param pd.DataFrame df: pandas dataframe to save
        :param str table_name: name of the database table to save data in
        :return:
        """

        df.to_sql(table_name, self.engine, index=False, if_exists='append')
        self._df = None

    def clean_data(self, data):
        """ converts categories to a numerical format and expands its columns, also remove duplciates
        :param pd.DataFrame data: pandas dataframe containing raw data of messages and categories
        :return: pd.DataFrame: pandas dataframe of clean categories and removed duplicates
        """
        df = data.copy()

        # extract categories from dataframe and expand its values
        cats = df.pop('categories').str.split(';', expand=True)

        # use the first row of df to generate proper column names
        # slice the names from first letter to 2nd-to-last letter
        cats.columns = cats.iloc[0].apply(lambda x: x[:-2])

        # convert the cats values to include only numbers
        cats = cats.apply(lambda col: pd.to_numeric(col.str.split('-').str[1]))

        # concatenate modified categories to df
        self._df = pd.concat([df, cats], axis=1)

        # remove duplicates
        self._df.drop_duplicates(inplace=True)

        return self._df


if __name__ == '__main__':
    import os

    dir_path = os.path.dirname(os.path.abspath(__file__))
    messages_filepath = os.path.join(dir_path, '../../data/disaster_messages.csv')
    categories_filepath = os.path.join(dir_path, '../../data/disaster_categories.csv')
    database_filepath = os.path.join(dir_path, '../../data/DisasterResponse.db')

    etl_pipe = ETLPipe(database_filepath)

    df = etl_pipe.load_data_csv(messages_filepath, categories_filepath, clean_data=True)
    print(etl_pipe.data)

    etl_pipe.save_data_db(df, 'messages')


