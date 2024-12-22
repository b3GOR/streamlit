import streamlit as st
import pandas as pd
from functions import anomaly,std_groupby,mean_groupby,plot,fetch_weather
import asyncio
import aiohttp

async def main():
    upload_files = st.file_uploader('Загрузите исторические данные',type=['csv'])
    if upload_files:
        df = pd.read_csv(upload_files)
        df = anomaly(df)
        city = st.selectbox('Выберите город', df['city'].unique())
        season = st.selectbox('Выберите сезон', df['season'].unique())
        st.header(f'Сезонный профиль для города {city}')
        st.write(df[(df['city']==city)&((df['season']==season))]['temperature'].describe())
        if st.checkbox('График температуры для данного города и сезона'):
            st.plotly_chart(plot(df,city=city,season=season))
        
        API = st.text_input('Введите API ключ от OpenWeatherMap')
        if st.button('Погода на данный момент в выбранном городе'):
            try:
                async with aiohttp.ClientSession() as session:
                    temp = await fetch_weather(session,city,API)
                    
                    std_group=std_groupby(df)
                    mean_group=mean_groupby(df)
                    mean = mean_group[mean_group.index==city][season].values[0]
                    std = std_group[std_group.index==city][season].values[0]
                    st.header(f'{temp} {chr(176)}C')
                    if (mean-2*std)<=temp<=(mean+2*std):
                    
                        st.write('Температура типична для данного города и сезона')
                    else:
                        st.write('Температура аномальна для данного города и сезона')
            except ValueError as e:
                st.error({"cod": 401, "message": e.args[0]})

if __name__=='__main__':
    asyncio.run(main())