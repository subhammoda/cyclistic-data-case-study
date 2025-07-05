import pandas as pd

class CleanData:

    def __init__(self) -> None:
        
        self.root_folder = "./Data"
        self.jun24 = pd.read_csv(f'{self.root_folder}/202406-divvy-tripdata.csv', index_col=0)
        self.jul24 = pd.read_csv(f'{self.root_folder}/202407-divvy-tripdata.csv', index_col=0)
        self.aug24 = pd.read_csv(f'{self.root_folder}/202408-divvy-tripdata.csv', index_col=0)
        self.sept24 = pd.read_csv(f'{self.root_folder}/202409-divvy-tripdata.csv', index_col=0)
        self.oct24 = pd.read_csv(f'{self.root_folder}/202410-divvy-tripdata.csv', index_col=0)
        self.nov24 = pd.read_csv(f'{self.root_folder}/202411-divvy-tripdata.csv', index_col=0)
        self.dec24 = pd.read_csv(f'{self.root_folder}/202412-divvy-tripdata.csv', index_col=0)
        self.jan25 = pd.read_csv(f'{self.root_folder}/202501-divvy-tripdata.csv', index_col=0)
        self.feb25 = pd.read_csv(f'{self.root_folder}/202502-divvy-tripdata.csv', index_col=0)
        self.mar25 = pd.read_csv(f'{self.root_folder}/202503-divvy-tripdata.csv', index_col=0)
        self.apr25 = pd.read_csv(f'{self.root_folder}/202504-divvy-tripdata.csv', index_col=0)
        self.may25 = pd.read_csv(f'{self.root_folder}/202505-divvy-tripdata.csv', index_col=0)

    def combine_data(self) -> None:

        self.data = pd.concat([self.jun24, self.jul24, self.aug24, self.sept24, self.oct24, self.nov24, self.dec24, self.jan25, self.feb25, self.mar25, self.apr25, self.may25])

        self.data = self.data.reset_index(level=0)
        
    def clean_data(self) -> pd.DataFrame:

        self.data['started_at'] = pd.to_datetime(self.data['started_at'])
        self.data['ended_at'] = pd.to_datetime(self.data['ended_at'])
        
        self.data_no_null = self.data.dropna()
        
        self.df_ride = self.data_no_null.copy(deep=True)
        self.df_ride['ride_time[s]'] = (self.df_ride['ended_at'] - self.df_ride['started_at']).dt.total_seconds()

        self.df_ride = self.df_ride[self.df_ride['ride_time[s]'] > 0]

        self.df_clean = self.df_ride[~((self.df_ride['start_station_id'] == self.df_ride['end_station_id']) & (self.df_ride['ride_time[s]'] < 60))]

        self.df_final = self.df_clean.copy(deep=True)
        self.df_final['Weekday'] = self.df_final['started_at'].dt.weekday

        new_columns = {
            'rideable_type':'BikeType',
            'started_at': 'RideStart',
            'ended_at': 'RideEnd',
            'ride_time[s]': 'RideTime[s]',
            'start_station_name': 'StartStation',
            'end_station_name': 'EndStation',
            'member_casual': 'UserType'
        }

        self.df_final.rename(columns=new_columns, inplace=True)

        return self.df_final
    
if __name__ == '__main__':
    cd = CleanData()
    cd.combine_data()
    df = cd.clean_data()
    print(df.shape)