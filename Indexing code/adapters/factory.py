from .sparql_adapter import SPARQLAdapter
from .dump_adapter import RDFDumpAdapter
from .tpf_adapter import TPFAdapter

# adapters/factory.py

class AdapterFactory:
    @staticmethod
    def create(source, mode="sparql", engine=None): 
        if mode == "sparql":
            if engine is None:
                raise ValueError("SPARQLAdapter requires engine instance")
            return SPARQLAdapter(source, engine)  
        elif mode == "dump":
            return RDFDumpAdapter(source)
        elif mode == "tpf":
            return TPFAdapter(source)
        else:
            raise ValueError(f"Unknown mode: {mode}")