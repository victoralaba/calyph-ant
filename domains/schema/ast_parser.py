# domains/schema/ast_parser.py

from loguru import logger

try:
    from pglast import parse_sql
    from pglast.visitors import Visitor
except ImportError:
    parse_sql = None
    Visitor = object
    logger.warning("pglast not installed. AST parsing will fail gracefully.")

class ASTAnalyzer(Visitor):
    """
    Traverses a PostgreSQL AST to deterministically extract the target objects
    (tables, indexes, sequences, etc.) affected by the DDL statements.
    
    This replaces heuristic regex tokenization, ensuring we perfectly ignore
    comments, string literals, and complex dollar-quoted bodies.
    """
    
    def __init__(self):
        # Avoid calling super().__init__() if Visitor is just `object` during ImportError
        if Visitor is not object:
            super().__init__()
        self.targets = set()

    def visit_RangeVar(self, node):
        """
        RangeVar represents a table, view, sequence, or index reference.
        This is the most common and robust way to find targeted relation names.
        """
        if hasattr(node, "relname") and node.relname:
            self.targets.add(node.relname.lower())
        # Continue traversing children if any (usually RangeVar is a leaf)
        if hasattr(self, "generic_visit"):
            self.generic_visit(node)

    def visit_IndexStmt(self, node):
        """Extract explicit index names created by CREATE INDEX."""
        if hasattr(node, "idxname") and node.idxname:
            self.targets.add(node.idxname.lower())
        if hasattr(self, "generic_visit"):
            self.generic_visit(node)

    def visit_RenameStmt(self, node):
        """Extract both the old and new names when renaming an object."""
        if hasattr(node, "subname") and node.subname:
            self.targets.add(node.subname.lower())
        if hasattr(node, "newname") and node.newname:
            self.targets.add(node.newname.lower())
        if hasattr(self, "generic_visit"):
            self.generic_visit(node)

    def visit_CreateEnumStmt(self, node):
        """Extract enum names from CREATE TYPE ... AS ENUM."""
        if hasattr(node, "typeName") and node.typeName:
            # typeName is usually a list of String nodes
            pass # handled by generic_visit -> String
        if hasattr(self, "generic_visit"):
            self.generic_visit(node)

    def visit_String(self, node):
        """
        Identifiers in DROP statements (DropStmt) are often represented as
        simple String nodes inside the 'objects' list.
        """
        if hasattr(node, "sval") and node.sval:
            self.targets.add(node.sval.lower())
        elif hasattr(node, "str") and node.str:
            self.targets.add(node.str.lower())
        if hasattr(self, "generic_visit"):
            self.generic_visit(node)

    def extract_targets(self, sql: str) -> set[str]:
        """
        Parse the SQL and traverse its AST, returning a set of all extracted
        object names (tables, indexes, sequences, etc.) in lowercase.
        """
        if parse_sql is None:
            logger.warning("pglast is not installed. AST extraction returning empty set.")
            return set()
            
        self.targets = set()
        try:
            root = parse_sql(sql)
            self(root)
        except Exception as exc:
            logger.warning(f"AST parsing failed for SQL snippet: {exc}")
        
        return self.targets


def extract_objects_from_sql(sql: str) -> set[str]:
    """
    Public entrypoint for extracting target object names from a SQL string.
    """
    analyzer = ASTAnalyzer()
    return analyzer.extract_targets(sql)
