import itertools

dataset = ['hk', 'xj', 'blm', 'debate']
# dataset = ['blm', 'debate']
dimension = ['hashtags', 'selected_hashtags', 'pacheco_cs3']
keep_top_interactions_percentile = [0.005, 0.01, 0.02, 0.05, 0.1, 0.2, 0.5, 1.]
edge_filter_percent = [1 - p for p in keep_top_interactions_percentile]

dataset_params = {
    'hk': {'min_num_tweets': 1, 'min_num_hashtags': 1},
    'xj': {'min_num_tweets': 1, 'min_num_hashtags': 1},
    'blm': {'min_num_tweets': 1, 'min_num_hashtags': 1},
    'debate': {'min_num_tweets': 1, 'min_num_hashtags': 1}
}

p1_col = 'uid'
p2_col = 'feature'
w_col = 'cnt'
num_top_features = 40

interaction_n1_col = 'user1'
interaction_n2_col = 'user2'
interaction_sim_col = 'similarity'
interaction_sup_col = 'support'

rule all:
    input:
        # expand("figures/{dataset}/{dimension}/{percent}/coord.network.pdf",
        #        dataset=dataset, dimension=dimension, percent=edge_filter_percent),
        expand("features/summary.csv", dimension=dimension),



rule combine_summary_tables:
    input:
        "tcd/combine_summary.py",
        summaries = [
            f"features/{d}/summary.csv" 
            for d in dimension
            ]
    output:
        "features/summary.csv"
    shell:
        """
        python3 -m tcd.combine_summary -i {input.summaries} -o {output}
        """

rule summary_table:
    input:
        "tcd/summary.py",
        group_stats = [
            f"features/{d}/{{dimension}}/{p}/group_stats.json" 
            for d,p in set(itertools.product(dataset, edge_filter_percent))
            ]
    output:
        "features/{dimension}/summary.csv"
    shell:
        """
        python3 -m tcd.summary -i {input.group_stats} -o {output}
        """

rule compute_group_statistics:
    input:
        "tcd/group_stats.py",
        group="features/{dataset}/{dimension}/{percent}/group.json",
        authors="features/{dataset}/cleaned_authors.parquet"
    output: 
        "features/{dataset}/{dimension}/{percent}/group_stats.json"
    shell:
        """
        python3 -m tcd.group_stats -g {input.group} -a {input.authors} -o {output}
        """

rule combine_groups:
    threads: 4
    input:
        "tcd/combine.py",
        interaction="features/{dataset}/{dimension}/interactions.parquet",
    output:
        graph="features/{dataset}/{dimension}/{percent}/filtered.coord.graphml",
        group="features/{dataset}/{dimension}/{percent}/group.json"
    shell:
        """
        python3 -m tcd.combine -i {input.interaction} \
                -o {output.graph} -g {output.group} \
                --node1 {interaction_n1_col} --node2 {interaction_n2_col} \
                --sim {interaction_sim_col} --sup {interaction_sup_col} \
                --min-interaction-percent {wildcards.percent} \
                --min-centrality-percent 0


        """

rule measure_interaction:
    input:
        "tcd/measure.py",
        edges="features/{dataset}/{dimension}/edge.parquet"
    output:
        "features/{dataset}/{dimension}/interactions.parquet"
    shell:
        """
        python3 -m tcd.measure -i {input.edges} -o {output} \
                --p1col {p1_col} --p2col {p2_col} --wcol {w_col} \
                --node1 {interaction_n1_col} --node2 {interaction_n2_col} \
                --sim {interaction_sim_col} --sup {interaction_sup_col}
        """

rule clean_authors:
    input:
        "tcd/clean_authors.py",
        authors="data/{dataset}/authors.csv"
    output:
        "features/{dataset}/cleaned_authors.parquet"
    shell:
        """
        python3 -m tcd.clean_authors -i {input.authors} -o {output}
        """

# --------------------------------------------------
# Rules for creating edges
# --------------------------------------------------

# Create edges from hashtag counts
rule create_hashtag_edge_files:
    input:
        script="tcd/create_edges.py",
        hashtag_counts="data/{dataset}/hashtag_counts.csv",
        top_hashtags="data/{dataset}/top_hashtags.csv"
    output:
        edge="features/{dataset}/{dimension}/edge.parquet"
    params:
        outdir="features/{dataset}"
    wildcard_constraints:
        dimension="hashtags|selected_hashtags"
    shell:
        """
        python3 -m tcd.create_edges --hashtag_counts {input.hashtag_counts} --top-hashtags {input.top_hashtags} -o {params.outdir} \
                --p1col {p1_col} --p2col {p2_col} --wcol {w_col} --num-top-features {num_top_features}
        """

# Create edges from Pacheco et al. method, case study 3
rule create_pacheco_edge_files:
    input:
        script="tcd/create_pacheco_edges_v2.py",
        tweets="data/{dataset}/tweets.csv"
    output:
        edge="features/{dataset}/pacheco_cs3/edge.parquet"
    params:
        min_num_tweets=lambda wildcards: dataset_params[wildcards.dataset]['min_num_tweets'],
        min_num_hashtags=lambda wildcards: dataset_params[wildcards.dataset]['min_num_hashtags']
    wildcard_constraints:
        dimension="pacheco_cs3"
    shell:
        """
        python3 -m tcd.create_pacheco_edges_v2 -i {input.tweets} -o {output.edge} \
            --min-num-tweets {params.min_num_tweets} \
            --min-num-hashtags {params.min_num_hashtags}
        """

# create edges from flags only
rule create_flagonly_edge_files:
    input:
        script="tcd/create_flagonly_edges.py",
        authors="data/{dataset}/authors.csv"
    output:
        edge="features/{dataset}/flagonly/edge.parquet"
    shell:
        """
        python3 -m {input.script} -i {input.authors} -o {output.edge}
        """



# --------------------------------------------------
# Utilities
# --------------------------------------------------

rule count_raw_file_num_line:
    input:
        "data/{dataset}/raw_tweets.json.gz"
    output:
        "data/{dataset}/raw_tweets.numline"
    shell:
        "zcat {input} | wc -l > {output}"

# create a DAG of the rules
rule make_dag:
    input:
        "Snakefile"
    output:
        dag_rules = "dag_rules.pdf",
        dag_full = "dag_full.pdf",
    shell:
        """
        snakemake --forceall --rulegraph | dot -Tpdf > {output.dag_rules}
        snakemake --forceall --dag | dot -Tpdf > {output.dag_full}
        """



# rule graph_tool_visualize:
#     input:
#         "tcd/gtgraph.py",
#         graph="features/{dataset}/{dimension}/{percent}/filtered.coord.graphml"
#     output:
#         pdf="figures/{dataset}/{dimension}/{percent}/coord.network.pdf"
#     shell:
#         """
#         python3 -m tcd.gtgraph -i {input.graph} -o {output.pdf}
#         """