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
count = 0
for tweet in tweets_col.find(filter_, projection=projection):
    author_name = tweet["authorName"]
    if "retweetedTweet" in tweet:
        original_author = tweet["retweetedTweet"]["authorName"]
        if original_author not in kol_usernames:
            kol_usernames.append(original_author)
            data[original_author] = {"username": original_author, "mentioned": 0, "retweeted": 0}
            if author_name != original_author:
                data[original_author]["retweeted"] += 1
                edges.append((author_name, original_author))
                weights.append(0.8)

    if "quotedTweet" in tweet:
        original_author = tweet["quotedTweet"]["authorName"]
        if original_author not in kol_usernames:
            kol_usernames.append(original_author)
            data[original_author] = {"username": original_author, "mentioned": 0, "retweeted": 0}
            if author_name != original_author:
                count += 1
                data[original_author]["retweeted"] += 1
                edges.append((author_name, original_author))
                weights.append(0.8)

    if "userMentions" in tweet:
        for _id, username in tweet["userMentions"].items():
            if username not in kol_usernames:
                kol_usernames.append(username)
                data[username] = {"username": username, "mentioned": 0, "retweeted": 0}
                if author_name != username:
                    data[username]["mentioned"] += 1
                    edges.append((author_name, username))
                    weights.append(0.6)
print(count)
# Result: 8976

## S4: Count the number of tweets that are quoted
tweets_col.count_documents({
    "timestamp": {"$gte": start_time, "$lte": end_time},
    "authorName": {"$in": kol_usernames},
    "quotedTweet": { "$exists": True }
})
# Result: 8976

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
# (0-10]: 125984
# (10-20]: 0
# (20-40]: 0
# (40-60]: 0
# (60-80]: 0
# (80-100]: 0
# (100-200]: 0
# (200-300]: 0
# (300-400]: 0
# (400-500]: 0
# (500-1000]: 0
# (1000-20000]: 0

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
count = 0
for edge in edges:
    count += 1
    if count == 10:
        break
    print(edge)
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

print(pagerank_scores)
# Result:
# array([6.25512921e-06, 6.25512921e-06, 6.25512921e-06, ...,
#        7.37446812e-06, 7.43665361e-06, 7.50615505e-06])


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
    #     break
    print(f"Hạng {i+1}: {username} - {score:.4f}")
    results[username] = score
# Result:
# Hạng 1: CryptoCwby - 0.1157
# Hạng 2: PiedPiperAI - 0.1157
# Hạng 3: nasmithan - 0.1157
# Hạng 4: Gegagedigedag - 0.1157
# Hạng 5: enginayyildiz28 - 0.1157
# Hạng 6: hodliss - 0.1157
# Hạng 7: IrrespBored - 0.1157
# Hạng 8: zapcatofficial - 0.1157
# Hạng 9: BuddyEvap - 0.1157
# Hạng 10: masoncags - 0.1157
# Hạng 11: 0xVictorP - 0.1157
# Hạng 12: official_doom - 0.1157
# Hạng 13: BlockcastNet - 0.1157
# Hạng 14: russ5555555 - 0.1157
# Hạng 15: holddoteth - 0.1157
# Hạng 16: FilmgateMiami - 0.1157
# Hạng 17: DogeXRPL - 0.1157
# Hạng 18: ArloHotels - 0.1157
# Hạng 19: Lalalunalina - 0.1157
# Hạng 20: meowptain - 0.1157

print(results)
# Result:
# {'saylor': 75.84631247970893, 'blknoiz06': 48.50169320989287, 'cz_binance': 46.73929848416769, 'brian_armstrong': 38.54178482186251, 'notthreadguy': 35.49393289631007, 'C

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
# (0-1]: 18314
# (1-2]: 1120
# (2-3]: 349
# (3-4]: 156
# (4-5]: 63
# (5-10]: 141
# (10-20]: 44
# (20-40]: 19
# (40-60]: 2
# (60-80]: 1
# (80-100]: 0

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