dataset = ['hk', 'xj', 'blm', 'debate']
dimension = ['hashtags', 'selected_hashtags']

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
        expand("features/{dataset}/cleaned_authors.parquet", dataset=dataset),
        expand("features/{dataset}/{dimension}.filtered.coord.graphml",
               dataset=dataset, dimension=dimension),
        expand("figures/{dataset}/{dimension}.coord.network.pdf",
               dataset=dataset, dimension=dimension),
        expand("features/{dataset}/{dimension}.group.json",
               dataset=dataset, dimension=dimension),
        expand("features/{dataset}/{dimension}.group_stats.json",
               dataset=dataset, dimension=dimension),
        expand("features/{dimension}.summary.csv", dimension=dimension),
        [expand(f"features/{{dataset}}/{dim}.edge.parquet", dataset=dataset) for dim in dimension]

rule graph_tool_visualize:
    input:
        "tcd/gtgraph.py",
        graph="features/{dataset}/{dimension}.filtered.coord.graphml"
    output:
        pdf="figures/{dataset}/{dimension}.coord.network.pdf"
    shell:
        """
        python3 -m tcd.gtgraph -i {input.graph} -o {output.pdf}
        """

rule summary_table:
    input:
        "tcd/summary.py",
        group_stats = [
            f"features/{d}/{{dimension}}.group_stats.json" for d in dataset
            ]
    output:
        "features/{dimension}.summary.csv"
    shell:
        """
        python3 -m tcd.summary -i {input.group_stats} -o {output}
        """

rule compute_group_statistics:
    input:
        "tcd/group_stats.py",
        group="features/{dataset}/{dimension}.group.json",
        authors="features/{dataset}/cleaned_authors.parquet"
    output: 
        "features/{dataset}/{dimension}.group_stats.json"
    shell:
        """
        python3 -m tcd.group_stats -g {input.group} -a {input.authors} -o {output}
        """

rule combine_groups:
    input:
        "tcd/combine.py",
        interaction="features/{dataset}/{dimension}.interactions.parquet",
    output:
        graph="features/{dataset}/{dimension}.filtered.coord.graphml",
        group="features/{dataset}/{dimension}.group.json"
    shell:
        """
        python3 -m tcd.combine -i {input.interaction} \
                -o {output.graph} -g {output.group} \
                --node1 {interaction_n1_col} --node2 {interaction_n2_col} \
                --sim {interaction_sim_col} --sup {interaction_sup_col}
        """

rule measure_interaction:
    input:
        "tcd/measure.py",
        edges="features/{dataset}/{dimension}.edge.parquet"
    output:
        "features/{dataset}/{dimension}.interactions.parquet"
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

rule create_edge_files:
    input:
        "tcd/create_edges.py",
        hashtag_counts="data/{dataset}/hashtag_counts.csv",
        top_hashtags="data/{dataset}/top_hashtags.csv",
    output:
        [f"features/{{dataset}}/{dim}.edge.parquet" for dim in dimension]
    params:
        outdir="features/{dataset}"
    run:
        shell("""
            python3 -m tcd.create_edges --hashtag_counts {input.hashtag_counts} --top-hashtags {input.top_hashtags} -o {params.outdir} \
                    --p1col {p1_col} --p2col {p2_col} --wcol {w_col} --num-top-features {num_top_features}
        """)

rule count_raw_file_num_line:
    input:
        "data/{dataset}/raw_tweets.json.gz"
    output:
        "data/{dataset}/raw_tweets.numline"
    shell:
        "zcat {input} | wc -l > {output}"
