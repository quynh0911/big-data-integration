import networkx as nx
import matplotlib.pyplot as plt
import numpy as np

# Create a graph
G = nx.DiGraph()

# Add all nodes first
for node in nodes:
    G.add_node(node)

# Add all edges with their weights
for (source, target), weight in zip(edges, weights):
    G.add_edge(source, target, weight=weight)

# Create visualization
plt.figure(figsize=(20, 20))

# Use a force-directed layout
pos = nx.spring_layout(G, k=1/np.sqrt(len(G)), iterations=50)

# Calculate node sizes based on degree centrality
degree_dict = dict(G.degree())
node_sizes = [v * 2 for v in degree_dict.values()]

# Draw the network
nx.draw_networkx_nodes(G, pos, node_size=node_sizes, alpha=0.6, node_color='skyblue')
nx.draw_networkx_edges(G, pos, alpha=0.1, arrows=False)  # Set alpha low for better visibility with many edges

plt.title(f"KOL Network\n{len(nodes)} nodes, {len(edges)} edges", pad=20)
plt.axis('off')

# Save with high resolution
plt.savefig('kol_network_full.png', dpi=300, bbox_inches='tight')
plt.close()

print(f"Network visualization saved as 'kol_network_full.png'")
print(f"Network statistics:")
print(f"Number of nodes: {len(G.nodes())}")
print(f"Number of edges: {len(G.edges())}")
print(f"Network density: {nx.density(G):.6f}")
print(f"Average degree: {sum(dict(G.degree()).values())/len(G):.2f}")

# Calculate and display top 10 nodes by degree
top_degrees = sorted(degree_dict.items(), key=lambda x: x[1], reverse=True)[:10]
print("\nTop 10 nodes by degree:")
for node, degree in top_degrees:
    print(f"{node}: {degree} connections") 