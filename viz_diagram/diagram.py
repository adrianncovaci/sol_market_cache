import matplotlib.pyplot as plt
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import math 

data = {
    'program_id': [
        'srmqPvymJeFKQ4zGQed1GFppgkRHL9kaELCbyksJtPX',
        '675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8',
        'CAMMCzo5YL8w4VFF8KVHrK22GGUsp5VTaW7grrKgrWqK',
        'PERPHjGBqRHArX4DySjwM6UJHiR3sWAatqfdBS2qQJu',
        'LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo',
        'whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc',
        'FLUXubRmkEi2q6K3Y9kBPg9248ggaZVsoSFhtJHSrm1X',
        'PhoeNiXZ8ByJGLkxNfZRnkUfjvmuYqLR89jjFHGqdXY',
        'Eo7WjKq67rjJQSZxS6z3YkapzY3eMj6Xy8X5EQVn5UaB',
        'CPMMoo8L3F4NbTegBCKVNunggL7H1ZpdTHKxQB5qKP1C',
        'CURVGoZn8zycx6FXwwevgBTB2gVvdbGTEpvMJDbgs2t4',
        'AMM55ShdkoGRB5jVYPjWziwk8m5MpwyDgsMWHaMSQWH6',
        'opnb2LAfJYbRMAHHvqjCwQxanZn7ReEHp1k81EohpZb',
        'BSwp6bEBihVLdqJRKGgzjcGLHkcTuzmSo1TQkHepzH8p',
        'HyaB3W9q6XdA5xwpU4XnSZV94htfmbmqJXZcEbRaJutt',
        'DSwpgjMvXhtGn6BsbqmacdBZyfLj6jSWf3HJpdJtmg6N',
        'CLMM9tUoggJu2wagPkkqs9eFG4BWhVBZWkP1qv3Sp7tR',
        'H8W3ctz92svYg6mkn1UtGfu2aQr2fnUFHM1RhScEtQDt',
        'SSwapUtytfBdBn1b9NUGG6foMVPtcWgpRU32HToDUZr',
        'SwaPpA9LAaLfeLi3a68M4DjnLqgtticKg6CnyNwgAC8',
        '9W959DqEETiGZocYWCQPaJ6sBmUzgfxXfqGeTEdp3aQP',
        'SSwpkEEcbUqx4vtoEByFjSkhKdCT862DNVb52nZg1UZ',
        'CTMAxxk34HjKWxQ3QLZK1HpaLXmBveao3ESePXbiyfzh',
        '9tKE7Mbmj4mxDjWatikzGAtkoWosiiZX9y6J4Hfm2R8H',
        'Gswppe6ERWKpUTXvRPfXdzHhiCyJvLadVvXGfdpBqcE1',
        '2wT8Yq49kHgDzXuPxZSaeLaH1qbmGXtEyPy64bL7aD3c',
        'obriQD1zbpyLz95G5n7nJe6a4DPjpFwa5XYPoNm113y',
        'DEXYosS6oEGvk8uCDayvwEZz4qEyDJRf9nFgYCaqPMTm',
        'swapNyd8XiQwJ6ianp9snpu4brUqFxadzvHebnAXjJZ',
        'PSwapMdSai8tjrEXcxFeQth87xC4rRsa4VA5mhGhXkP',
        'DjVE6JNiYqPL2QXyCUUh8rNjHrbz9hXHNYt99MQ59qw1',
        'swapFpHZwjELNnjvThjajtiVmkz3yPQEHjLtka2fwHW',
        'MERLuDFBMmsHnsBPZw2sDQZHvXFMwp8EdjudcU2HKky',
        'DecZY86MU5Gj7kppfUCEmd4LbXXuyZH1yHaP2NTqdiZB',
        'treaf4wWBBty3fHdyBpo35Mz84M8k3heKXmjmi9vFt5',
        'Dooar9JkhdZ7J3LHN3A7YCuoGRUggXhQaG4kijfLGU2j',
        '5ocnV1qiCgaQR8Jb8xWnVbApfaygJ8tNoZfgPwsgx9kx',
        'stkitrT1Uoy18Dk1fTrgPw8W6MVzoCfYoAFT4MLsmhq'
    ],
    'accounts': [
        3183333, 991459, 680260, 505418, 309586, 224443, 157751, 95613,
        52661, 41970, 37753, 29764, 2976, 1997, 1836, 1544, 686, 537,
        507, 337, 167, 163, 153, 140, 138, 61, 37, 32, 32, 28, 27, 26,
        24, 23, 12, 4, 2, 0
    ],
    'fetch_time': [
        290, 83, 64, 72, 86, 19, 30, 9, 11, 4, 5, 4, 1, 1, 1, 1, 1, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0
    ]
}

# Create DataFrame and sort by number of accounts
df = pd.DataFrame(data)
# Sort dataframe by accounts
df_sorted = df.sort_values('accounts')

# Create the subplot figure
fig = make_subplots(
    rows=1, cols=2,
    subplot_titles=(
        '<b>Number of Accounts</b>', 
        '<b>Fetch Time (seconds)</b>'
    ),
    horizontal_spacing=0.15,
)

# Add accounts trace
fig.add_trace(
    go.Bar(
        x=df_sorted['accounts'],
        y=df_sorted['program_id'],
        orientation='h',
        text=df_sorted['accounts'].apply(lambda x: f'{int(x):,}'),
        textposition='outside',
        name='Accounts',
        hovertemplate='Program: %{y}<br>Accounts: %{x:,}<extra></extra>'
    ),
    row=1, col=1
)

# Add fetch time trace
fig.add_trace(
    go.Bar(
        x=df_sorted['fetch_time'],
        y=df_sorted['program_id'],
        orientation='h',
        text=df_sorted['fetch_time'].apply(lambda x: f'{int(x)}s' if x > 0 else '0s'),
        textposition='outside',
        name='Fetch Time',
        hovertemplate='Program: %{y}<br>Time: %{x}s<extra></extra>'
    ),
    row=1, col=2
)

# Update layout
fig.update_layout(
    height=1200,  # Increased height
    width=1800,   # Increased width
    showlegend=False,
    title_text="<b>Program Analytics</b>",
    title_x=0.5,
    paper_bgcolor='white',
    plot_bgcolor='white',
)

# Update x-axes
fig.update_xaxes(
    title_text="Number of Accounts",
    type='log',
    showgrid=True,
    gridwidth=1,
    gridcolor='LightGray',
    row=1, col=1
)

fig.update_xaxes(
    title_text="Fetch Time (seconds)",
    type='log',
    showgrid=True,
    gridwidth=1,
    gridcolor='LightGray',
    row=1, col=2,
    range=[-1, math.log10(max(df_sorted['fetch_time']) * 1.2)]
)

# Update y-axes to show all labels
fig.update_yaxes(
    tickmode='array',
    ticktext=df_sorted['program_id'],
    tickvals=list(range(len(df_sorted))),
    showgrid=True,
    gridwidth=1,
    gridcolor='LightGray',
)

# Save interactive version
fig.write_html('program_analytics.html')
