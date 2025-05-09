from pymongo import MongoClient

## S1: Get tweets of KOLs
client = MongoClient("mongodb://localhost:27017/")
db = client["cdp_database"]
tweets_col = db["tweets"]
users_col = db["twitter_raw"]

kol_usernames = users_col.distinct("userName", {"elite": True})
start_time = 1731628800
end_time = 1734220800
filters = {
    "timestamp": {"$gte": start_time, "$lte": end_time},
    "authorName": {"$in": kol_usernames}
}


pipeline = [
    {"$match": filters},
    {"$group": {
        "_id": "$authorName",
        "tweet_count": {"$sum": 1}
    }},
    {"$group": {
        "_id": None,
        "unique_authors": {"$sum": 1},
        "total_tweets": {"$sum": "$tweet_count"}
    }}
]


result = list(tweets_col.aggregate(pipeline))
if result:
    total_users = result[0]["unique_authors"]
    total_tweets = result[0]["total_tweets"]
    print(f"Total users: {total_users}, Total tweets: {total_tweets}")
else:
    print("No data found.")


# Result: 
# Total users: 19992, Total tweets: 1515138

## S2: Count the number of KOL
count_i = 0
for i in kol_usernames:
    count_i += 1
print(count_i)
# Result: 20210

data = {user: {"username": user, "mentioned": 0, "retweeted": 0} for user in kol_usernames}
# print(data)

## S3: Get edges and weights
filter_ = {
    "timestamp": {"$gte": start_time, "$lte": end_time},
    "authorName": {"$in": kol_usernames},
    "$or": [
        { "userMentions": { "$exists": True } },
        { "retweetedTweet": { "$exists": True } },
        {
            "$and": [
                { "userMentions": { "$exists": True } },
                { "retweetedTweet": { "$exists": True } }
            ]
        }
    ]
}
projection = {
    "authorName": 1,
    "userMentions": 1,
    "retweetedTweet": 1,
    "quotedTweet": 1
}
nodes = kol_usernames
edges = []
weights = []
post = 0
quote = 0
mention = 0
for tweet in tweets_col.find(filter_, projection=projection):
    author_name = tweet["authorName"]
    if "retweetedTweet" in tweet:
        original_author = tweet["retweetedTweet"]["authorName"]
        if author_name != original_author and original_author in kol_usernames:
            data[original_author]["retweeted"] += 1
            edges.append((author_name, original_author))
            weights.append(1)
            post += 1

    if "quotedTweet" in tweet:
        original_author = tweet["quotedTweet"]["authorName"]
        if author_name != original_author and original_author in kol_usernames:
            data[original_author]["retweeted"] += 1
            edges.append((author_name, original_author))
            weights.append(0.8)
            quote += 1

    if "userMentions" in tweet:
        for _id, username in tweet["userMentions"].items():
            if author_name != username and username in kol_usernames:
                data[username]["mentioned"] += 1
                edges.append((author_name, username))
                weights.append(0.6)
                mention += 1

print(f"Post: {post}, Quote: {quote}, Mention: {mention}")
# Post: 200943, Quote: 35637, Mention: 352419

## S4: Count the number of tweets that are quoted
tweets_col.count_documents({
    "timestamp": {"$gte": start_time, "$lte": end_time},
    "authorName": {"$in": kol_usernames},
    "quotedTweet": { "$exists": True }
})
# Result: 335239

## S5: Count the number of tweets that are mentioned
new_data = {}
for k, v in data.items():
    new_data[k] = v["mentioned"] + v["retweeted"]

## S6: Count the number of tweets that are mentioned in different bins
bins = [0, 10, 20, 40, 60, 80, 100, 200, 300, 400, 500, 1000, 20000]
bin_labels = [f"({bins[i]}-{bins[i+1]}]" for i in range(len(bins) - 1)]

counts = {label: 0 for label in bin_labels}
for username, value in new_data.items():
    for i in range(len(bins) - 1):
        if bins[i] < value <= bins[i + 1]:
            counts[bin_labels[i]] += 1
            break

for bin_range, count in counts.items():
    print(f"{bin_range}: {count}")

# Result:
# (0-10]: 7125
# (10-20]: 2744
# (20-40]: 2478
# (40-60]: 1213
# (60-80]: 684
# (80-100]: 432
# (100-200]: 860
# (200-300]: 194
# (300-400]: 103
# (400-500]: 42
# (500-1000]: 50
# (1000-20000]: 23

## S7: Plot the number of tweets that are mentioned in different bins (visualize)
import matplotlib.pyplot as plt

bins = list(counts.keys())
values = list(counts.values())

plt.figure(figsize=(12, 6))
plt.bar(bins, values, color='skyblue')

for i, value in enumerate(values):
    plt.text(i, value + max(values) * 0.01, str(value), ha='center', fontsize=10)

plt.xlabel('Edges', fontsize=12)
plt.ylabel('Nodes', fontsize=12)
plt.title('Exploratory Data Analysis', fontsize=14)
plt.xticks(rotation=45, ha='right')
plt.tight_layout()

plt.show()

## S8: Count the number of edges
print(len(nodes), len(edges))
# Result: 20210 588999

# Result:
# ('KittenHaimer', 'geojamofficial')
# ('soro', 'G7_DAO')
# ('soro', 'Immutable')
# ('bitpinas', '0xGenie')
# ('bitpinas', 'ismaeljerusalem')
# ('bitpinas', 'Duck_Chain')
# ('bitpinas', 'blumcrypto')
# ('bitpinas', 'bitgetglobal')

## S9: Calculate the PageRank score
from scipy import sparse
from fast_pagerank import pagerank
from fast_pagerank import pagerank_power
import numpy as np

num_nodes = len(nodes)

node_to_index = {node: index for index, node in enumerate(nodes)}

edges_indices = [(node_to_index[u], node_to_index[v]) for u, v in edges]


A = np.array(edges_indices)
G = sparse.csr_matrix((weights, (A[:, 0], A[:, 1])), shape=(num_nodes, num_nodes))

print("Tính PageRank")
damping_factor = 0.85
pagerank_scores = pagerank(G, p=damping_factor)

pagerank_scores
# Result:
# array([1.75449571e-05, 5.78929984e-05, 9.06709123e-06, ...,
#       9.06709123e-06, 9.49529449e-05, 1.00280675e-05])


## S10: Sort the PageRank scores
import numpy as np

sorted_indices = np.argsort(pagerank_scores)[::-1]
sorted_scores = pagerank_scores[sorted_indices]
sorted_nodes = [nodes[i] for i in sorted_indices]


scaled_scores = sorted_scores * 10000

rank = 0
results = {}
for i, (username, score) in enumerate(zip(sorted_nodes, scaled_scores)):
    rank += 1
    # if rank > 10:
        # break
    print(f"Hạng {i+1}: {username} - {score:.4f}")
    results[username] = score
# Result:
# Hạng 1: aixbt_agent - 81.4286
# Hạng 2: saylor - 74.3875
# Hạng 3: blknoiz06 - 44.7501
# Hạng 4: cz_binance - 43.6360
# Hạng 5: jessepollak - 34.5796
# Hạng 6: notthreadguy - 33.9554
# Hạng 7: brian_armstrong - 33.3316
# Hạng 8: CynthiaMLummis - 32.2249
# Hạng 9: truth_terminal - 30.6033
# Hạng 10: chooserich - 30.4212

print(results)
# Result:
# {'aixbt_agent': np.float64(81.42860313540118), 'saylor': np.float64(74.3875464871092), 'blknoiz06': np.float64(44.75014912272566), 'cz_binance': np.float64(43.63603887980668), 'jessepollak': np.float64(34.579603586200214), 'notthreadguy': np.float64(33.955366021685734), 'brian_armstrong': np.float64(33.331563587284016), 'CynthiaMLummis': np.float64(32.224885436271286), 'truth_terminal': np.float64(30.603347968145638), 'chooserich': np.float64(30.421238219417642)}

## S11: Count the number of nodes in different bins
bins = [0, 1, 2, 3, 4, 5, 10, 20, 40, 60, 80, 100]
bin_labels = [f"({bins[i]}-{bins[i+1]}]" for i in range(len(bins) - 1)]

counts = {label: 0 for label in bin_labels}
for username, value in results.items():
    for i in range(len(bins) - 1):
        if bins[i] < value <= bins[i + 1]:
            counts[bin_labels[i]] += 1
            break

for bin_range, count in counts.items():
    print(f"{bin_range}: {count}")
# Result:
# (0-1]: 18353
# (1-2]: 1112
# (2-3]: 329
# (3-4]: 144
# (4-5]: 67
# (5-10]: 135
# (10-20]: 44
# (20-40]: 22
# (40-60]: 2
# (60-80]: 1
# (80-100]: 1

## S12: Plot the number of nodes in different bins (visualize)
import matplotlib.pyplot as plt

bins = list(counts.keys())
values = list(counts.values())

plt.figure(figsize=(12, 6))
plt.bar(bins, values, color='skyblue')

for i, value in enumerate(values):
    plt.text(i, value + max(values) * 0.01, str(value), ha='center', fontsize=10)

plt.xlabel('Score Range', fontsize=12)
plt.ylabel('Number of Nodes', fontsize=12)
plt.title('Exploratory Data Analysis', fontsize=14)
plt.xticks(rotation=45, ha='right')
plt.tight_layout()

plt.show()

## S13: Plot the number of nodes in different bins (visualize)
line_data = results

# Extracting keys and values for line plot
names = list(line_data.keys())
values = list(line_data.values())

# Plotting the line chart
plt.figure(figsize=(12, 6))
# plt.plot(names, values, marker='o', linestyle='-', color='skyblue')
# plt.scatter(names, values, color='skyblue')
plt.scatter(range(len(values)), values, color='skyblue')

# Adding labels and title
plt.xlabel('User', fontsize=12)
plt.ylabel('Score', fontsize=12)
# plt.title('Line Chart of Data', fontsize=14)
# plt.grid(True, linestyle='--', alpha=0.6)

# Show the plot
plt.tight_layout()
plt.show()

## S14: Count the number of followers
mapping_follower = {}
for kol in results.keys():
    mapping_follower[kol] = users_col.find_one({"userName": kol})["followersCount"]

print(mapping_follower)


