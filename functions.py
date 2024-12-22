
import plotly.express as px
import aiohttp


def anomaly(df):
    mean_group=df.groupby(['city','season'])['temperature'].mean().reset_index().pivot_table(index='city', columns='season', values='temperature')
    std_group=df.groupby(['city','season'])['temperature'].std().reset_index().pivot_table(index='city', columns='season', values='temperature')
    for x in std_group.columns:
        for y in std_group.index:
                mean = mean_group[mean_group.index==y][x].iloc[0].round(2)
                std = std_group[std_group.index==y][x].iloc[0].round(2)
                condition = (df['city']==y) & (df['season']==x)
                df.loc[condition, 'anomaly'] = df.loc[condition, 'temperature'].apply(lambda x: 0 if (mean - 2 * std)<=x <= (mean + 2 * std) else 1) 
    df['anomaly']=df['anomaly'].astype(int)
    return df

def std_groupby(df):
    return df.groupby(['city','season'])['temperature'].std().reset_index().pivot_table(index='city', columns='season', values='temperature')

def mean_groupby(df):
    return df.groupby(['city','season'])['temperature'].mean().reset_index().pivot_table(index='city', columns='season', values='temperature')
def plot(df,season,city):
    filtered_data = df.query(f"(season == '{season}') & (city == '{city}')")
    fig = px.scatter(filtered_data, x='timestamp', y=f'temperature', title=f"График температуры для {city}, {season}")
    fig.update_yaxes(title_text= f'temperature ({chr(176)}C)')
    anomalies = filtered_data.query("anomaly == 1")
    fig.add_scatter(x=anomalies['timestamp'], y=anomalies['temperature'], mode='markers', 
                    marker=dict(color='red', size=10),name='Anomaly',showlegend=False)
    return fig

async def fetch_weather(session, city, api_key):
    url = f"https://api.openweathermap.org/data/2.5/weather?q={city}&units=metric&appid={api_key}"
    try:
        async with session.get(url) as response:
            if response.status == 200:
                result = await response.json()
                try:
                    temp = result["main"]["temp"]
                    return temp
                except KeyError:
                    print(f"Не удалось получить температуру для {city}. Ответ API не содержит данных о температуре.")
                    return None
            elif response.status == 401:
                error_message = await response.json()
                raise ValueError(f"Ошибка API (401): {error_message['message']}")
            elif response.status == 404:
                print(f"Город {city} не найден. Проверьте правильность написания.")
                return None
            else:
                print(f"Произошла ошибка при запросе погоды для {city}. Статус-код: {response.status}")
                return None
    except aiohttp.ClientError as e:
        print(f"Ошибка при подключении к OpenWeatherMap для города {city}: {e}")
        return None

