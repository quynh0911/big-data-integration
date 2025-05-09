import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
import pandas as pd
import networkx as nx
import json
from datetime import datetime, timedelta
from collections import Counter
import re
import ijson
import os

# Purpose: Setup visualization settings for consistent charts
sns.set_style('whitegrid')
sns.set_context('paper')
np.random.seed(42)  # For reproducibility

# Purpose: Load and preprocess Twitter KOL data from JSON source
def load_data_sample(file_path='datn_data/cdp_db.twitter_users.json'):
    print("Loading Twitter users data...")
    users = []
    with open(file_path, 'rb') as f:
        parser = ijson.items(f, 'item')
        for i, user in enumerate(parser):
            if user.get('engagementChangeLogs') and user.get('followersCount', 0) > 1000:  # Filter for active users
                users.append({
                    '_id': user['_id'],
                    'userName': user.get('userName', ''),
                    'followersCount': user.get('followersCount', 0),
                    'engagement_metrics': process_engagement_logs(user.get('engagementChangeLogs', {})),
                    'tweet_counts': process_tweet_counts(user.get('tweetCountChangeLogs', {})),
                    'view_counts': process_view_counts(user.get('viewChangeLogs', {}))
                })
            if i >= 999:  # Limit to 1000 users
                break
    users_df = pd.DataFrame(users)
    print(f"Loaded {len(users_df)} users with engagement data")
    return users_df

# Purpose: Convert raw engagement logs into structured time series data
def process_engagement_logs(logs):
    if not logs:
        return {'likes': [], 'replies': [], 'retweets': [], 'dates': []}
    
    daily_metrics = {
        'likes': [],
        'replies': [],
        'retweets': [],
        'dates': []
    }
    
    for timestamp, metrics in sorted(logs.items()):
        daily_metrics['dates'].append(int(timestamp))
        daily_metrics['likes'].append(metrics.get('likeCount', 0))
        daily_metrics['replies'].append(metrics.get('replyCount', 0))
        daily_metrics['retweets'].append(metrics.get('retweetCount', 0))
    
    return daily_metrics

# Purpose: Process tweet count logs for activity analysis
def process_tweet_counts(logs):
    if not logs:
        return {}
    return {int(k): v for k, v in logs.items()}

# Purpose: Process view count logs to analyze content reach
def process_view_counts(logs):
    if not logs:
        return {}
    return {int(k): v for k, v in logs.items()}

# Purpose: Analyze distribution of engagement levels across KOLs to identify patterns
def analyze_engagement_distribution(users_df):
    engagement_data = []
    tweet_frequency = []
    
    for _, user in users_df.iterrows():
        if user['engagement_metrics'].get('likes'):
            avg_engagement = np.mean([
                sum(x) for x in zip(
                    user['engagement_metrics']['likes'],
                    user['engagement_metrics']['replies'],
                    user['engagement_metrics']['retweets']
                )
            ])
            engagement_data.append(avg_engagement)
        
        if user['tweet_counts']:
            avg_tweets = np.mean(list(user['tweet_counts'].values()))
            tweet_frequency.append(avg_tweets)
            
    return {
        'engagement_stats': {
            'mean': np.mean(engagement_data),
            'median': np.median(engagement_data),
            'std': np.std(engagement_data),
            'data': engagement_data
        },
        'frequency_stats': {
            'mean': np.mean(tweet_frequency),
            'median': np.median(tweet_frequency),
            'std': np.std(tweet_frequency),
            'data': tweet_frequency
        }
    }

# Purpose: Track engagement trends for top KOLs to identify patterns and anomalies
def analyze_engagement_trends(users_df, top_n=5):
    top_users = users_df.nlargest(top_n, 'followersCount')
    trends = {}
    
    for _, user in top_users.iterrows():
        if user['engagement_metrics'].get('dates'):
            dates = [datetime.fromtimestamp(ts) for ts in user['engagement_metrics']['dates']]
            engagement = [sum(x) for x in zip(
                user['engagement_metrics']['likes'],
                user['engagement_metrics']['replies'],
                user['engagement_metrics']['retweets']
            )]
            trends[user['userName']] = {
                'dates': dates,
                'engagement': engagement
            }
    
    return trends

# Purpose: Calculate influence scores to rank KOLs by their impact
def calculate_influence_scores(users_df):
    users_df['influence_score'] = users_df.apply(lambda x: 
        np.mean([sum(y) for y in zip(
            x['engagement_metrics'].get('likes', [0]),
            x['engagement_metrics'].get('replies', [0]),
            x['engagement_metrics'].get('retweets', [0])
        )]) * np.log10(x['followersCount'] + 1) if x['engagement_metrics'].get('likes') else 0,
        axis=1
    )
    
    # Create influence categories
    influence_thresholds = users_df['influence_score'].quantile([0.33, 0.66])
    users_df['influence_category'] = users_df['influence_score'].apply(
        lambda x: 'High' if x >= influence_thresholds[0.66]
        else ('Medium' if x >= influence_thresholds[0.33] else 'Low')
    )
    
    return users_df

# Purpose: Build network visualization of KOL interactions to identify key influencer clusters
def build_kol_network(users_df, top_n=30, correlation_threshold=0.6):
    top_users = users_df.nlargest(top_n, 'followersCount')
    G = nx.Graph()
    
    # Add nodes
    for _, user in top_users.iterrows():
        G.add_node(user['userName'] or f"User-{user['_id'][:6]}", 
                  followers=user['followersCount'],
                  influence=user.get('influence_score', 0))
    
    # Add edges based on similar engagement patterns
    for i, user1 in top_users.iterrows():
        for j, user2 in top_users.iloc[i+1:].iterrows():
            if (user1['engagement_metrics'].get('likes') and 
                user2['engagement_metrics'].get('likes') and
                len(user1['engagement_metrics']['likes']) > 5 and
                len(user2['engagement_metrics']['likes']) > 5):
                try:
                    # Calculate total engagement for each user
                    user1_engagement = [sum(x) for x in zip(
                        user1['engagement_metrics']['likes'],
                        user1['engagement_metrics']['replies'],
                        user1['engagement_metrics']['retweets']
                    )]
                    
                    user2_engagement = [sum(x) for x in zip(
                        user2['engagement_metrics']['likes'],
                        user2['engagement_metrics']['replies'],
                        user2['engagement_metrics']['retweets']
                    )]
                    
                    # Calculate correlation if data is valid
                    if np.std(user1_engagement) > 0 and np.std(user2_engagement) > 0:
                        correlation = np.corrcoef(user1_engagement, user2_engagement)[0, 1]
                        if not np.isnan(correlation) and correlation > correlation_threshold:
                            G.add_edge(user1['userName'] or f"User-{user1['_id'][:6]}", 
                                      user2['userName'] or f"User-{user2['_id'][:6]}", 
                                      weight=correlation)
                except Exception as e:
                    # Skip if correlation calculation fails
                    print(f"  Skipping edge due to: {e}")
                    pass
    
    return G

# Purpose: Analyze temporal engagement patterns to identify optimal posting times
def analyze_temporal_patterns(users_df):
    hourly_engagement = {i: 0 for i in range(24)}
    daily_engagement = {i: 0 for i in range(7)}
    
    for _, user in users_df.iterrows():
        if user['engagement_metrics'].get('dates'):
            for ts in user['engagement_metrics']['dates']:
                dt = datetime.fromtimestamp(ts)
                hourly_engagement[dt.hour] += 1
                daily_engagement[dt.weekday()] += 1
    
    return {
        'hourly': hourly_engagement,
        'daily': daily_engagement
    }

# Purpose: Apply PageRank algorithm to identify most influential KOLs in the network
def apply_pagerank(G, damping_factor=0.85):
    pagerank_scores = nx.pagerank(G, alpha=damping_factor)
    return pagerank_scores

# Purpose: Analyze correlation between KOL activity and market movements
def analyze_market_impact(kol_activity, market_data):
    # Calculate correlation
    correlation = np.corrcoef(kol_activity, market_data)[0, 1]
    
    # Calculate lag correlations (how KOL activity predicts future price movements)
    lag_correlations = []
    for lag in range(1, 8):  # Check correlations up to 7 days lag
        lag_correlation = np.corrcoef(
            kol_activity[:-lag], 
            market_data[lag:]
        )[0, 1]
        lag_correlations.append((lag, lag_correlation))
    
    return {
        'direct_correlation': correlation,
        'lag_correlations': lag_correlations
    }

# Purpose: Main function to run the complete KOL analysis pipeline
def main():
    # Create output directory if it doesn't exist
    if not os.path.exists('analysis_results'):
        os.makedirs('analysis_results')
    
    try:
        # Step 1: Load data
        print("Loading data from database...")
        users_df = load_data_sample()
        
        # Step 2: Analyze engagement distribution
        print("Analyzing engagement distribution...")
        engagement_stats = analyze_engagement_distribution(users_df)
        
        # Step 3: Calculate influence scores
        print("Calculating influence scores...")
        users_df = calculate_influence_scores(users_df)
        
        # Step 4: Build KOL network
        print("Building KOL network...")
        G = build_kol_network(users_df)
        
        # Step 5: Apply PageRank to identify most influential KOLs
        print("Applying PageRank algorithm...")
        pagerank_scores = apply_pagerank(G)
        
        # Step 6: Analyze temporal patterns
        print("Analyzing temporal patterns...")
        temporal_patterns = analyze_temporal_patterns(users_df)
        
        # Step 7: Export results
        print("Exporting results...")
        # Save top KOLs by influence score
        top_kols = users_df.nlargest(50, 'influence_score')[['userName', 'followersCount', 'influence_score', 'influence_category']]
        top_kols.to_csv('analysis_results/top_kols_by_influence.csv', index=False)
        
        # Save top KOLs by PageRank
        pagerank_df = pd.DataFrame([
            {'userName': node, 'pagerank_score': score} 
            for node, score in pagerank_scores.items()
        ]).sort_values('pagerank_score', ascending=False)
        pagerank_df.to_csv('analysis_results/top_kols_by_pagerank.csv', index=False)
        
        print("Analysis completed successfully!")
        
    except Exception as e:
        print(f"Error during analysis: {e}")

if __name__ == "__main__":
    main()