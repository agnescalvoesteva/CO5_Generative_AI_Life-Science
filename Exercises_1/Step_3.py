#Step_3
from whoosh import index
from step_1 import medline_folder
from Step_2 import index_dir, schema
from whoosh.qparser import MultifieldParser, OrGroup, AndGroup, FuzzyTermPlugin, PhrasePlugin

# Load your index here
ix = index.open_dir(index_dir)

with ix.searcher() as searcher: # we call searcher() in my index created
    # OR_PARSER, with the phrase queries or fuzzy/extract_match
    or_parser = MultifieldParser(["title", "body"], schema=ix.schema, group=OrGroup)
    or_parser.add_plugin(FuzzyTermPlugin()) # Fuzzy vs Exact Match
    or_parser.add_plugin(PhrasePlugin())
    print("OR executed")
    #AND_PARSER, with the phrase queries or fuzzy/extract_match
    and_parser = MultifieldParser(["title", "body"], schema=ix.schema, group=AndGroup)
    and_parser.add_plugin(FuzzyTermPlugin())
    and_parser.add_plugin(PhrasePlugin())
    print("AND executed")
    
    # Search here
    queries = [
       ("OR query", "protein", or_parser),
       ("AND query", "protein", and_parser),
       ("Phrase query", '"protein microorganism"', and_parser),
       ("Exact match", "protein", or_parser),
       ("Fuzzy match", "proteiniia~", or_parser),
       ]
    for name, query_string, parser in queries:
        print(f"\n--- {name} ---")
        print("Query:", query_string)

        query = parser.parse(query_string)
        results = searcher.search(query, limit=5)

        print("Results found:", len(results))

        for r in results:
            print("-", r["title"])
        print("End testing")
