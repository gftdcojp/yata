#![allow(dead_code)]

pub mod csr;

pub use error::*;
pub use types::*;
pub use ast::*;
pub use graph::*;
pub use executor::*;

// Re-export parse as a convenience function
pub use parser::parse;

// ---- error --------------------------------------------------------------

pub mod error {
    #[derive(thiserror::Error, Debug)]
    pub enum CypherError {
        #[error("parse error: {0}")]
        ParseError(String),
        #[error("type error: {0}")]
        TypeError(String),
        #[error("unbound variable: {0}")]
        UnboundVariable(String),
        #[error("graph error: {0}")]
        GraphError(String),
    }

    pub type Result<T> = std::result::Result<T, CypherError>;
}

// ---- types --------------------------------------------------------------

pub mod types {
    use indexmap::IndexMap;
    use std::fmt;

    #[derive(Clone, Debug, PartialEq)]
    pub struct NodeRef {
        pub id: String,
        pub labels: Vec<String>,
        pub props: IndexMap<String, Value>,
    }

    #[derive(Clone, Debug, PartialEq)]
    pub struct RelRef {
        pub id: String,
        pub rel_type: String,
        pub src: String,
        pub dst: String,
        pub props: IndexMap<String, Value>,
    }

    #[derive(Clone, Debug)]
    pub enum Value {
        Null,
        Bool(bool),
        Int(i64),
        Float(f64),
        Str(String),
        List(Vec<Value>),
        Map(IndexMap<String, Value>),
        Node(NodeRef),
        Rel(RelRef),
    }

    impl PartialEq for Value {
        fn eq(&self, other: &Self) -> bool {
            self.eq_val(other)
        }
    }

    impl Value {
        pub fn is_truthy(&self) -> bool {
            match self {
                Value::Null => false,
                Value::Bool(b) => *b,
                Value::Int(i) => *i != 0,
                Value::Float(f) => *f != 0.0,
                Value::Str(s) => !s.is_empty(),
                Value::List(l) => !l.is_empty(),
                Value::Map(m) => !m.is_empty(),
                Value::Node(_) => true,
                Value::Rel(_) => true,
            }
        }

        pub fn eq_val(&self, other: &Value) -> bool {
            match (self, other) {
                (Value::Null, Value::Null) => true,
                (Value::Bool(a), Value::Bool(b)) => a == b,
                (Value::Int(a), Value::Int(b)) => a == b,
                (Value::Float(a), Value::Float(b)) => a == b,
                (Value::Int(a), Value::Float(b)) => (*a as f64) == *b,
                (Value::Float(a), Value::Int(b)) => *a == (*b as f64),
                (Value::Str(a), Value::Str(b)) => a == b,
                (Value::List(a), Value::List(b)) => {
                    a.len() == b.len() && a.iter().zip(b.iter()).all(|(x, y)| x.eq_val(y))
                }
                (Value::Map(a), Value::Map(b)) => {
                    if a.len() != b.len() {
                        return false;
                    }
                    for (k, v) in a {
                        match b.get(k) {
                            Some(bv) => {
                                if !v.eq_val(bv) {
                                    return false;
                                }
                            }
                            None => return false,
                        }
                    }
                    true
                }
                (Value::Node(a), Value::Node(b)) => a.id == b.id,
                (Value::Rel(a), Value::Rel(b)) => a.id == b.id,
                _ => false,
            }
        }

        pub fn type_name(&self) -> &'static str {
            match self {
                Value::Null => "Null",
                Value::Bool(_) => "Boolean",
                Value::Int(_) => "Integer",
                Value::Float(_) => "Float",
                Value::Str(_) => "String",
                Value::List(_) => "List",
                Value::Map(_) => "Map",
                Value::Node(_) => "Node",
                Value::Rel(_) => "Relationship",
            }
        }

        /// Compare for ordering — returns None if incomparable
        pub fn partial_cmp_val(&self, other: &Value) -> Option<std::cmp::Ordering> {
            match (self, other) {
                (Value::Int(a), Value::Int(b)) => a.partial_cmp(b),
                (Value::Float(a), Value::Float(b)) => a.partial_cmp(b),
                (Value::Int(a), Value::Float(b)) => (*a as f64).partial_cmp(b),
                (Value::Float(a), Value::Int(b)) => a.partial_cmp(&(*b as f64)),
                (Value::Str(a), Value::Str(b)) => a.partial_cmp(b),
                _ => None,
            }
        }
    }

    impl fmt::Display for Value {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            match self {
                Value::Null => write!(f, "null"),
                Value::Bool(b) => write!(f, "{}", b),
                Value::Int(i) => write!(f, "{}", i),
                Value::Float(v) => write!(f, "{}", v),
                Value::Str(s) => write!(f, "{}", s),
                Value::List(items) => {
                    write!(f, "[")?;
                    for (i, v) in items.iter().enumerate() {
                        if i > 0 {
                            write!(f, ", ")?;
                        }
                        write!(f, "{}", v)?;
                    }
                    write!(f, "]")
                }
                Value::Map(m) => {
                    write!(f, "{{")?;
                    for (i, (k, v)) in m.iter().enumerate() {
                        if i > 0 {
                            write!(f, ", ")?;
                        }
                        write!(f, "{}: {}", k, v)?;
                    }
                    write!(f, "}}")
                }
                Value::Node(n) => write!(f, "({})", n.id),
                Value::Rel(r) => write!(f, "[{}:{}]", r.id, r.rel_type),
            }
        }
    }

    #[derive(Clone, Debug)]
    pub struct Row(pub IndexMap<String, Value>);

    #[derive(Clone, Debug)]
    pub struct ResultSet {
        pub columns: Vec<String>,
        pub rows: Vec<Row>,
    }

    impl ResultSet {
        pub fn empty() -> Self {
            Self {
                columns: Vec::new(),
                rows: Vec::new(),
            }
        }
    }
}

// ---- ast ----------------------------------------------------------------

pub mod ast {
    #[derive(Clone, Debug)]
    pub enum Literal {
        Null,
        Bool(bool),
        Int(i64),
        Float(f64),
        Str(String),
    }

    #[derive(Clone, Debug)]
    pub enum BinOp {
        Add,
        Sub,
        Mul,
        Div,
        Mod,
        Eq,
        Neq,
        Lt,
        Lte,
        Gt,
        Gte,
        And,
        Or,
        Xor,
        Concat,
    }

    #[derive(Clone, Debug)]
    pub enum UnOp {
        Neg,
        Not,
    }

    #[derive(Clone, Debug)]
    pub enum Expr {
        Lit(Literal),
        Var(String),
        Prop(Box<Expr>, String),
        Param(String),
        BinOp(BinOp, Box<Expr>, Box<Expr>),
        UnOp(UnOp, Box<Expr>),
        IsNull(Box<Expr>),
        IsNotNull(Box<Expr>),
        In(Box<Expr>, Box<Expr>),
        FnCall(String, Vec<Expr>),
        List(Vec<Expr>),
        Map(Vec<(String, Expr)>),
        Case {
            scrutinee: Option<Box<Expr>>,
            whens: Vec<(Expr, Expr)>,
            else_: Option<Box<Expr>>,
        },
        StartsWith(Box<Expr>, Box<Expr>),
        EndsWith(Box<Expr>, Box<Expr>),
        Contains(Box<Expr>, Box<Expr>),
        RegexMatch(Box<Expr>, Box<Expr>),
        Index(Box<Expr>, Box<Expr>),
        ListComp {
            var: String,
            list_expr: Box<Expr>,
            filter: Option<Box<Expr>>,
            map_expr: Option<Box<Expr>>,
        },
        /// Map projection: `n { .name, .age, key: expr, .* }`
        MapProjection {
            base: Box<Expr>,
            entries: Vec<MapProjEntry>,
        },
        /// EXISTS { subquery }
        ExistsSubquery(Vec<Clause>),
        /// List predicate functions: ALL/ANY/NONE/SINGLE(var IN list WHERE pred)
        ListPredicate {
            kind: ListPredicateKind,
            var: String,
            list_expr: Box<Expr>,
            pred: Box<Expr>,
        },
    }

    #[derive(Clone, Debug)]
    pub enum MapProjEntry {
        /// `.name` shorthand
        Prop(String),
        /// `key: expr` explicit
        Literal(String, Expr),
        /// `.*` all properties
        AllProps,
    }

    #[derive(Clone, Debug)]
    pub enum ListPredicateKind {
        All,
        Any,
        None,
        Single,
    }

    #[derive(Clone, Debug)]
    pub enum RelDir {
        Left,
        Right,
        Both,
    }

    #[derive(Clone, Debug)]
    pub struct NodePattern {
        pub var: Option<String>,
        pub labels: Vec<String>,
        pub props: Vec<(String, Expr)>,
    }

    #[derive(Clone, Debug)]
    pub struct RelPattern {
        pub var: Option<String>,
        pub types: Vec<String>,
        pub props: Vec<(String, Expr)>,
        pub dir: RelDir,
        pub min_hops: Option<u32>,
        pub max_hops: Option<u32>,
    }

    #[derive(Clone, Debug)]
    pub enum PatternElement {
        Node(NodePattern),
        Rel(RelPattern),
    }

    #[derive(Clone, Debug)]
    pub struct Pattern {
        pub elements: Vec<PatternElement>,
        /// Named path variable: `p = (a)-[r]->(b)`
        pub path_var: Option<String>,
    }

    #[derive(Clone, Debug)]
    pub struct ReturnItem {
        pub expr: Expr,
        pub alias: Option<String>,
    }

    #[derive(Clone, Debug)]
    pub struct OrderItem {
        pub expr: Expr,
        pub asc: bool,
    }

    #[derive(Clone, Debug)]
    pub enum SetItem {
        PropSet(Expr, Expr),
        LabelSet(String, Vec<String>),
    }

    #[derive(Clone, Debug)]
    pub enum Clause {
        Match {
            patterns: Vec<Pattern>,
            where_: Option<Expr>,
        },
        OptionalMatch {
            patterns: Vec<Pattern>,
            where_: Option<Expr>,
        },
        Create {
            patterns: Vec<Pattern>,
        },
        Merge {
            pattern: Pattern,
            on_create: Vec<SetItem>,
            on_match: Vec<SetItem>,
        },
        Return {
            items: Vec<ReturnItem>,
            distinct: bool,
            order_by: Vec<OrderItem>,
            limit: Option<Expr>,
            skip: Option<Expr>,
        },
        With {
            items: Vec<ReturnItem>,
            where_: Option<Expr>,
        },
        Set {
            items: Vec<SetItem>,
        },
        Delete {
            exprs: Vec<Expr>,
            detach: bool,
        },
        Unwind {
            expr: Expr,
            alias: String,
        },
        Union {
            all: bool,
        },
        Foreach {
            var: String,
            list_expr: Expr,
            body: Vec<Clause>,
        },
        Remove {
            items: Vec<SetItem>,
        },
        /// CALL { subquery }
        Call {
            subquery: Vec<Clause>,
        },
        /// CALL procedure(args) YIELD col1, col2
        CallProcedure {
            name: String,
            args: Vec<Expr>,
            yields: Vec<String>,
        },
    }

    #[derive(Clone, Debug)]
    pub struct Query {
        pub clauses: Vec<Clause>,
    }
}

// ---- lexer --------------------------------------------------------------

pub mod lexer {
    use crate::error::{CypherError, Result};

    #[derive(Clone, Debug, PartialEq)]
    pub enum Token {
        // Keywords
        Match,
        Optional,
        Create,
        Merge,
        Return,
        With,
        Where,
        Set,
        Delete,
        Detach,
        And,
        Or,
        Not,
        Xor,
        In,
        Is,
        Null,
        True,
        False,
        As,
        Distinct,
        Order,
        By,
        Asc,
        Desc,
        Limit,
        Skip,
        Unwind,
        On,
        Union,
        Foreach,
        Remove,
        Call,
        Yield,
        Exists,
        Case,
        When,
        Then,
        Else,
        End,
        // Punctuation
        LParen,
        RParen,
        LBracket,
        RBracket,
        LBrace,
        RBrace,
        Colon,
        Comma,
        Dot,
        Semicolon,
        Pipe,
        Arrow,      // ->
        LeftArrow,  // <-
        Dash,       // -
        DoubleDash, // --
        // Operators
        Eq,
        Neq,
        Lt,
        Lte,
        Gt,
        Gte,
        Plus,
        Minus,
        Star,
        Slash,
        Percent,
        PlusEq,      // +=
        RegexMatch,  // =~
        // Literals
        Ident(String),
        IntLit(i64),
        FloatLit(f64),
        StrLit(String),
        Param(String),
        // Control
        Eof,
    }

    pub struct Lexer {
        chars: Vec<char>,
        pub pos: usize,
        pub peeked: Option<Token>,
    }

    impl Lexer {
        pub fn new(input: &str) -> Self {
            Self {
                chars: input.chars().collect(),
                pos: 0,
                peeked: None,
            }
        }

        fn cur(&self) -> Option<char> {
            self.chars.get(self.pos).copied()
        }

        fn peek_char(&self) -> Option<char> {
            self.chars.get(self.pos + 1).copied()
        }

        fn advance(&mut self) -> Option<char> {
            let c = self.chars.get(self.pos).copied();
            self.pos += 1;
            c
        }

        fn skip_whitespace_and_comments(&mut self) {
            loop {
                // Skip whitespace
                while matches!(self.cur(), Some(c) if c.is_ascii_whitespace()) {
                    self.advance();
                }
                // Skip // line comments
                if self.cur() == Some('/') && self.peek_char() == Some('/') {
                    while matches!(self.cur(), Some(c) if c != '\n') {
                        self.advance();
                    }
                } else {
                    break;
                }
            }
        }

        fn read_string(&mut self, quote: char) -> Result<Token> {
            let mut s = String::new();
            loop {
                match self.advance() {
                    None => {
                        return Err(CypherError::ParseError(
                            "unterminated string literal".into(),
                        ))
                    }
                    Some(c) if c == quote => break,
                    Some('\\') => match self.advance() {
                        Some('n') => s.push('\n'),
                        Some('t') => s.push('\t'),
                        Some('r') => s.push('\r'),
                        Some('\\') => s.push('\\'),
                        Some('\'') => s.push('\''),
                        Some('"') => s.push('"'),
                        Some(c) => {
                            s.push('\\');
                            s.push(c);
                        }
                        None => {
                            return Err(CypherError::ParseError(
                                "unterminated escape sequence".into(),
                            ))
                        }
                    },
                    Some(c) => s.push(c),
                }
            }
            Ok(Token::StrLit(s))
        }

        fn read_backtick_ident(&mut self) -> Result<Token> {
            let mut s = String::new();
            loop {
                match self.advance() {
                    None => {
                        return Err(CypherError::ParseError(
                            "unterminated backtick identifier".into(),
                        ))
                    }
                    Some('`') => break,
                    Some(c) => s.push(c),
                }
            }
            Ok(Token::Ident(s))
        }

        fn read_number(&mut self, first: char) -> Token {
            let mut s = String::new();
            s.push(first);
            let mut is_float = false;
            loop {
                match self.cur() {
                    Some(c) if c.is_ascii_digit() => {
                        s.push(c);
                        self.advance();
                    }
                    Some('.') if !is_float && matches!(self.peek_char(), Some(c) if c.is_ascii_digit()) => {
                        is_float = true;
                        s.push('.');
                        self.advance();
                    }
                    Some('e') | Some('E') if !is_float => {
                        is_float = true;
                        s.push('e');
                        self.advance();
                        if matches!(self.cur(), Some('+') | Some('-')) {
                            s.push(self.advance().unwrap());
                        }
                    }
                    _ => break,
                }
            }
            if is_float {
                Token::FloatLit(s.parse().unwrap_or(0.0))
            } else {
                Token::IntLit(s.parse().unwrap_or(0))
            }
        }

        fn keyword_or_ident(s: String) -> Token {
            match s.to_ascii_uppercase().as_str() {
                "MATCH" => Token::Match,
                "OPTIONAL" => Token::Optional,
                "CREATE" => Token::Create,
                "MERGE" => Token::Merge,
                "RETURN" => Token::Return,
                "WITH" => Token::With,
                "WHERE" => Token::Where,
                "SET" => Token::Set,
                "DELETE" => Token::Delete,
                "DETACH" => Token::Detach,
                "AND" => Token::And,
                "OR" => Token::Or,
                "NOT" => Token::Not,
                "XOR" => Token::Xor,
                "IN" => Token::In,
                "IS" => Token::Is,
                "NULL" => Token::Null,
                "TRUE" => Token::True,
                "FALSE" => Token::False,
                "AS" => Token::As,
                "DISTINCT" => Token::Distinct,
                "ORDER" => Token::Order,
                "BY" => Token::By,
                "ASC" => Token::Asc,
                "ASCENDING" => Token::Asc,
                "DESC" => Token::Desc,
                "DESCENDING" => Token::Desc,
                "LIMIT" => Token::Limit,
                "SKIP" => Token::Skip,
                "UNWIND" => Token::Unwind,
                "ON" => Token::On,
                "UNION" => Token::Union,
                "FOREACH" => Token::Foreach,
                "REMOVE" => Token::Remove,
                "CALL" => Token::Call,
                "YIELD" => Token::Yield,
                "EXISTS" => Token::Exists,
                "CASE" => Token::Case,
                "WHEN" => Token::When,
                "THEN" => Token::Then,
                "ELSE" => Token::Else,
                "END" => Token::End,
                _ => Token::Ident(s),
            }
        }

        pub fn next_token(&mut self) -> Result<Token> {
            if let Some(t) = self.peeked.take() {
                return Ok(t);
            }
            self.skip_whitespace_and_comments();

            let c = match self.cur() {
                None => return Ok(Token::Eof),
                Some(c) => c,
            };
            self.advance();

            match c {
                '(' => Ok(Token::LParen),
                ')' => Ok(Token::RParen),
                '[' => Ok(Token::LBracket),
                ']' => Ok(Token::RBracket),
                '{' => Ok(Token::LBrace),
                '}' => Ok(Token::RBrace),
                ':' => Ok(Token::Colon),
                ',' => Ok(Token::Comma),
                '.' => Ok(Token::Dot),
                ';' => Ok(Token::Semicolon),
                '|' => Ok(Token::Pipe),
                '*' => Ok(Token::Star),
                '/' => Ok(Token::Slash),
                '%' => Ok(Token::Percent),
                '+' => {
                    if self.cur() == Some('=') {
                        self.advance();
                        Ok(Token::PlusEq)
                    } else {
                        Ok(Token::Plus)
                    }
                }
                '-' => {
                    if self.cur() == Some('-') {
                        self.advance();
                        Ok(Token::DoubleDash)
                    } else if self.cur() == Some('>') {
                        self.advance();
                        Ok(Token::Arrow)
                    } else {
                        Ok(Token::Dash)
                    }
                }
                '<' => {
                    if self.cur() == Some('-') {
                        self.advance();
                        Ok(Token::LeftArrow)
                    } else if self.cur() == Some('=') {
                        self.advance();
                        Ok(Token::Lte)
                    } else if self.cur() == Some('>') {
                        self.advance();
                        Ok(Token::Neq)
                    } else {
                        Ok(Token::Lt)
                    }
                }
                '>' => {
                    if self.cur() == Some('=') {
                        self.advance();
                        Ok(Token::Gte)
                    } else {
                        Ok(Token::Gt)
                    }
                }
                '=' => {
                    if self.cur() == Some('~') {
                        self.advance();
                        Ok(Token::RegexMatch)
                    } else {
                        Ok(Token::Eq)
                    }
                }
                '!' => {
                    if self.cur() == Some('=') {
                        self.advance();
                        Ok(Token::Neq)
                    } else {
                        Err(CypherError::ParseError(format!("unexpected char '!'")))
                    }
                }
                '\'' => self.read_string('\''),
                '"' => self.read_string('"'),
                '`' => self.read_backtick_ident(),
                '$' => {
                    let mut name = String::new();
                    while matches!(self.cur(), Some(c) if c.is_alphanumeric() || c == '_') {
                        name.push(self.advance().unwrap());
                    }
                    Ok(Token::Param(name))
                }
                c if c.is_ascii_digit() => Ok(self.read_number(c)),
                c if c.is_alphabetic() || c == '_' => {
                    let mut s = String::new();
                    s.push(c);
                    while matches!(self.cur(), Some(c) if c.is_alphanumeric() || c == '_') {
                        s.push(self.advance().unwrap());
                    }
                    Ok(Self::keyword_or_ident(s))
                }
                other => Err(CypherError::ParseError(format!(
                    "unexpected character: {:?}",
                    other
                ))),
            }
        }

        pub fn peek(&mut self) -> Result<&Token> {
            if self.peeked.is_none() {
                self.peeked = Some(self.next_token()?);
            }
            Ok(self.peeked.as_ref().unwrap())
        }
    }
}

// ---- parser -------------------------------------------------------------

pub mod parser {
    use crate::ast::*;
    use crate::error::{CypherError, Result};
    use crate::lexer::{Lexer, Token};

    pub fn parse(input: &str) -> Result<Query> {
        let mut p = Parser::new(input);
        p.parse_query()
    }

    pub struct Parser {
        lexer: Lexer,
    }

    impl Parser {
        pub fn new(input: &str) -> Self {
            Self {
                lexer: Lexer::new(input),
            }
        }

        fn peek(&mut self) -> Result<&Token> {
            self.lexer.peek()
        }

        fn next(&mut self) -> Result<Token> {
            self.lexer.next_token()
        }

        fn expect(&mut self, expected: &Token) -> Result<()> {
            let tok = self.next()?;
            if std::mem::discriminant(&tok) == std::mem::discriminant(expected) {
                Ok(())
            } else {
                Err(CypherError::ParseError(format!(
                    "expected {:?}, got {:?}",
                    expected, tok
                )))
            }
        }

        fn expect_ident(&mut self) -> Result<String> {
            match self.next()? {
                Token::Ident(s) => Ok(s),
                tok => Err(CypherError::ParseError(format!(
                    "expected identifier, got {:?}",
                    tok
                ))),
            }
        }

        /// Like expect_ident but also accepts keyword tokens as names.
        /// Used for labels, relationship types, and property keys where
        /// reserved words are commonly used as identifiers in Cypher.
        fn expect_name(&mut self) -> Result<String> {
            match self.next()? {
                Token::Ident(s) => Ok(s),
                // Allow any keyword as a name in label/type position
                Token::Match => Ok("Match".into()),
                Token::Optional => Ok("Optional".into()),
                Token::Create => Ok("Create".into()),
                Token::Merge => Ok("Merge".into()),
                Token::Return => Ok("Return".into()),
                Token::With => Ok("With".into()),
                Token::Where => Ok("Where".into()),
                Token::Set => Ok("Set".into()),
                Token::Delete => Ok("Delete".into()),
                Token::Detach => Ok("Detach".into()),
                Token::And => Ok("And".into()),
                Token::Or => Ok("Or".into()),
                Token::Not => Ok("Not".into()),
                Token::Xor => Ok("Xor".into()),
                Token::In => Ok("In".into()),
                Token::Is => Ok("Is".into()),
                Token::Null => Ok("Null".into()),
                Token::True => Ok("True".into()),
                Token::False => Ok("False".into()),
                Token::As => Ok("As".into()),
                Token::Distinct => Ok("Distinct".into()),
                Token::Order => Ok("Order".into()),
                Token::By => Ok("By".into()),
                Token::Asc => Ok("Asc".into()),
                Token::Desc => Ok("Desc".into()),
                Token::Limit => Ok("Limit".into()),
                Token::Skip => Ok("Skip".into()),
                Token::Unwind => Ok("Unwind".into()),
                Token::Case => Ok("Case".into()),
                Token::When => Ok("When".into()),
                Token::Then => Ok("Then".into()),
                Token::Else => Ok("Else".into()),
                Token::End => Ok("End".into()),
                Token::On => Ok("On".into()),
                Token::Union => Ok("Union".into()),
                Token::Foreach => Ok("Foreach".into()),
                Token::Remove => Ok("Remove".into()),
                Token::Call => Ok("Call".into()),
                Token::Yield => Ok("Yield".into()),
                Token::Exists => Ok("Exists".into()),
                tok => Err(CypherError::ParseError(format!(
                    "expected name, got {:?}",
                    tok
                ))),
            }
        }

        fn is_clause_start(tok: &Token) -> bool {
            matches!(
                tok,
                Token::Match
                    | Token::Optional
                    | Token::Create
                    | Token::Merge
                    | Token::Return
                    | Token::With
                    | Token::Set
                    | Token::Delete
                    | Token::Detach
                    | Token::Unwind
                    | Token::Union
                    | Token::Foreach
                    | Token::Remove
                    | Token::Call
            )
        }

        pub fn parse_query(&mut self) -> Result<Query> {
            let mut clauses = Vec::new();
            loop {
                // Skip semicolons
                while matches!(self.peek()?, Token::Semicolon) {
                    self.next()?;
                }
                if matches!(self.peek()?, Token::Eof) {
                    break;
                }
                clauses.push(self.parse_clause()?);
            }
            Ok(Query { clauses })
        }

        fn parse_clause(&mut self) -> Result<Clause> {
            match self.peek()? {
                Token::Match => self.parse_match(),
                Token::Optional => self.parse_optional_match(),
                Token::Create => self.parse_create(),
                Token::Merge => self.parse_merge(),
                Token::Return => self.parse_return(),
                Token::With => self.parse_with(),
                Token::Set => self.parse_set(),
                Token::Detach => self.parse_delete(),
                Token::Delete => self.parse_delete(),
                Token::Unwind => self.parse_unwind(),
                Token::Union => self.parse_union(),
                Token::Foreach => self.parse_foreach(),
                Token::Remove => self.parse_remove(),
                Token::Call => self.parse_call(),
                tok => Err(CypherError::ParseError(format!(
                    "unexpected token at clause start: {:?}",
                    tok
                ))),
            }
        }

        fn parse_match(&mut self) -> Result<Clause> {
            self.next()?; // consume MATCH
            let patterns = self.parse_pattern_list()?;
            let where_ = self.maybe_where()?;
            Ok(Clause::Match { patterns, where_ })
        }

        fn parse_optional_match(&mut self) -> Result<Clause> {
            self.next()?; // consume OPTIONAL
            // expect MATCH
            match self.next()? {
                Token::Match => {}
                tok => {
                    return Err(CypherError::ParseError(format!(
                        "expected MATCH after OPTIONAL, got {:?}",
                        tok
                    )))
                }
            }
            let patterns = self.parse_pattern_list()?;
            let where_ = self.maybe_where()?;
            Ok(Clause::OptionalMatch { patterns, where_ })
        }

        fn parse_create(&mut self) -> Result<Clause> {
            self.next()?; // consume CREATE
            let patterns = self.parse_pattern_list()?;
            Ok(Clause::Create { patterns })
        }

        fn parse_merge(&mut self) -> Result<Clause> {
            self.next()?; // consume MERGE
            let pattern = self.parse_pattern()?;
            let mut on_create = Vec::new();
            let mut on_match = Vec::new();
            loop {
                if !matches!(self.peek()?, Token::On) {
                    break;
                }
                self.next()?; // consume ON
                match self.peek()? {
                    Token::Create => {
                        self.next()?; // consume CREATE
                        self.expect(&Token::Set)?;
                        on_create = self.parse_set_items()?;
                    }
                    Token::Match => {
                        self.next()?; // consume MATCH
                        self.expect(&Token::Set)?;
                        on_match = self.parse_set_items()?;
                    }
                    _ => {
                        let tok = self.next()?;
                        return Err(CypherError::ParseError(format!(
                            "expected CREATE or MATCH after ON, got {:?}", tok
                        )));
                    }
                }
            }
            Ok(Clause::Merge { pattern, on_create, on_match })
        }

        fn parse_return(&mut self) -> Result<Clause> {
            self.next()?; // consume RETURN
            let distinct = if matches!(self.peek()?, Token::Distinct) {
                self.next()?;
                true
            } else {
                false
            };
            let items = self.parse_return_items()?;
            let order_by = self.maybe_order_by()?;
            let skip = self.maybe_skip()?;
            let limit = self.maybe_limit()?;
            Ok(Clause::Return {
                items,
                distinct,
                order_by,
                limit,
                skip,
            })
        }

        fn parse_with(&mut self) -> Result<Clause> {
            self.next()?; // consume WITH
            let items = self.parse_return_items()?;
            let where_ = self.maybe_where()?;
            Ok(Clause::With { items, where_ })
        }

        fn parse_set(&mut self) -> Result<Clause> {
            self.next()?; // consume SET
            let items = self.parse_set_items()?;
            Ok(Clause::Set { items })
        }

        fn parse_delete(&mut self) -> Result<Clause> {
            let detach = if matches!(self.peek()?, Token::Detach) {
                self.next()?;
                true
            } else {
                false
            };
            self.next()?; // consume DELETE
            let mut exprs = vec![self.parse_expr()?];
            while matches!(self.peek()?, Token::Comma) {
                self.next()?;
                exprs.push(self.parse_expr()?);
            }
            Ok(Clause::Delete { exprs, detach })
        }

        fn parse_unwind(&mut self) -> Result<Clause> {
            self.next()?; // consume UNWIND
            let expr = self.parse_expr()?;
            match self.next()? {
                Token::As => {}
                tok => {
                    return Err(CypherError::ParseError(format!(
                        "expected AS in UNWIND, got {:?}",
                        tok
                    )))
                }
            }
            let alias = self.expect_ident()?;
            Ok(Clause::Unwind { expr, alias })
        }

        fn parse_union(&mut self) -> Result<Clause> {
            self.next()?; // consume UNION
            let all = if matches!(self.peek()?, Token::Ident(s) if s.eq_ignore_ascii_case("ALL")) {
                self.next()?;
                true
            } else {
                false
            };
            Ok(Clause::Union { all })
        }

        fn parse_foreach(&mut self) -> Result<Clause> {
            self.next()?; // consume FOREACH
            self.expect(&Token::LParen)?;
            let var = self.expect_ident()?;
            self.expect(&Token::In)?;
            let list_expr = self.parse_expr()?;
            self.expect(&Token::Pipe)?;
            let mut body = Vec::new();
            while !matches!(self.peek()?, Token::RParen) {
                body.push(self.parse_clause()?);
            }
            self.expect(&Token::RParen)?;
            Ok(Clause::Foreach { var, list_expr, body })
        }

        fn parse_remove(&mut self) -> Result<Clause> {
            self.next()?; // consume REMOVE
            let items = self.parse_set_items()?;
            Ok(Clause::Remove { items })
        }

        fn parse_call(&mut self) -> Result<Clause> {
            self.next()?; // consume CALL
            // Distinguish CALL { subquery } from CALL procedure(args) YIELD ...
            if matches!(self.peek()?, Token::LBrace) {
                self.expect(&Token::LBrace)?;
                let mut subquery = Vec::new();
                while !matches!(self.peek()?, Token::RBrace) {
                    subquery.push(self.parse_clause()?);
                }
                self.expect(&Token::RBrace)?;
                Ok(Clause::Call { subquery })
            } else {
                // CALL procedure.name(args) YIELD col1, col2
                let mut name = self.expect_name()?;
                // Read dotted name: db.index.vector.queryNodes
                while matches!(self.peek()?, Token::Dot) {
                    self.next()?; // consume dot
                    let part = self.expect_name()?;
                    name.push('.');
                    name.push_str(&part);
                }
                self.expect(&Token::LParen)?;
                let mut args = Vec::new();
                if !matches!(self.peek()?, Token::RParen) {
                    args.push(self.parse_expr()?);
                    while matches!(self.peek()?, Token::Comma) {
                        self.next()?;
                        args.push(self.parse_expr()?);
                    }
                }
                self.expect(&Token::RParen)?;
                // YIELD is optional
                let mut yields = Vec::new();
                if matches!(self.peek()?, Token::Yield) {
                    self.next()?; // consume YIELD
                    yields.push(self.expect_name()?);
                    while matches!(self.peek()?, Token::Comma) {
                        self.next()?;
                        yields.push(self.expect_name()?);
                    }
                }
                Ok(Clause::CallProcedure { name, args, yields })
            }
        }

        fn maybe_where(&mut self) -> Result<Option<Expr>> {
            if matches!(self.peek()?, Token::Where) {
                self.next()?;
                Ok(Some(self.parse_expr()?))
            } else {
                Ok(None)
            }
        }

        fn maybe_order_by(&mut self) -> Result<Vec<OrderItem>> {
            if !matches!(self.peek()?, Token::Order) {
                return Ok(Vec::new());
            }
            self.next()?; // ORDER
            match self.next()? {
                Token::By => {}
                tok => {
                    return Err(CypherError::ParseError(format!(
                        "expected BY after ORDER, got {:?}",
                        tok
                    )))
                }
            }
            let mut items = vec![self.parse_order_item()?];
            while matches!(self.peek()?, Token::Comma) {
                self.next()?;
                items.push(self.parse_order_item()?);
            }
            Ok(items)
        }

        fn parse_order_item(&mut self) -> Result<OrderItem> {
            let expr = self.parse_expr()?;
            let asc = match self.peek()? {
                Token::Asc => {
                    self.next()?;
                    true
                }
                Token::Desc => {
                    self.next()?;
                    false
                }
                _ => true, // default ascending
            };
            Ok(OrderItem { expr, asc })
        }

        fn maybe_limit(&mut self) -> Result<Option<Expr>> {
            if matches!(self.peek()?, Token::Limit) {
                self.next()?;
                Ok(Some(self.parse_expr()?))
            } else {
                Ok(None)
            }
        }

        fn maybe_skip(&mut self) -> Result<Option<Expr>> {
            if matches!(self.peek()?, Token::Skip) {
                self.next()?;
                Ok(Some(self.parse_expr()?))
            } else {
                Ok(None)
            }
        }

        fn parse_pattern_list(&mut self) -> Result<Vec<Pattern>> {
            let mut patterns = vec![self.parse_pattern()?];
            while matches!(self.peek()?, Token::Comma) {
                self.next()?;
                // Make sure it looks like a pattern start
                if matches!(self.peek()?, Token::LParen) {
                    patterns.push(self.parse_pattern()?);
                } else {
                    break;
                }
            }
            Ok(patterns)
        }

        fn parse_pattern(&mut self) -> Result<Pattern> {
            // Check for named path: `p = (a)-[r]->(b)`
            let path_var = if let Token::Ident(_) = self.peek()?.clone() {
                let saved_pos = self.lexer.pos;
                let saved_peeked = self.lexer.peeked.clone();
                if let Token::Ident(name) = self.next()? {
                    if matches!(self.peek()?, Token::Eq) {
                        self.next()?; // consume =
                        Some(name)
                    } else {
                        // Not a named path, restore state
                        self.lexer.pos = saved_pos;
                        self.lexer.peeked = saved_peeked;
                        None
                    }
                } else {
                    None
                }
            } else {
                None
            };

            let mut elements = Vec::new();
            // Must start with a node
            elements.push(PatternElement::Node(self.parse_node_pattern()?));

            loop {
                // Check for relationship: -, <-, -[, <-[
                match self.peek()? {
                    Token::Dash | Token::DoubleDash | Token::LeftArrow | Token::LBracket => {
                        let rel = self.parse_rel_pattern()?;
                        elements.push(PatternElement::Rel(rel));
                        // After rel must come a node
                        if matches!(self.peek()?, Token::LParen) {
                            elements.push(PatternElement::Node(self.parse_node_pattern()?));
                        } else {
                            break;
                        }
                    }
                    _ => break,
                }
            }
            Ok(Pattern { elements, path_var })
        }

        fn parse_node_pattern(&mut self) -> Result<NodePattern> {
            self.expect(&Token::LParen)?;
            let var = match self.peek()? {
                Token::Ident(_) => {
                    if let Token::Ident(s) = self.next()? {
                        Some(s)
                    } else {
                        unreachable!()
                    }
                }
                _ => None,
            };
            let mut labels = Vec::new();
            while matches!(self.peek()?, Token::Colon) {
                self.next()?;
                labels.push(self.expect_name()?);
            }
            let props = if matches!(self.peek()?, Token::LBrace) {
                self.parse_props_map()?
            } else {
                Vec::new()
            };
            self.expect(&Token::RParen)?;
            Ok(NodePattern { var, labels, props })
        }

        fn parse_rel_pattern(&mut self) -> Result<RelPattern> {
            // Possible patterns:
            //   -[...]->, -[...]-, <-[...]-
            //   -->  , --  , <--
            //   ->   , -   , <-
            let dir_left = match self.peek()? {
                Token::LeftArrow => {
                    self.next()?;
                    true
                }
                _ => false,
            };

            // Skip leading dash(es)
            if matches!(self.peek()?, Token::Dash | Token::DoubleDash) {
                self.next()?;
            }

            let (var, types, props, min_hops, max_hops) =
                if matches!(self.peek()?, Token::LBracket) {
                    self.next()?; // consume [
                    let var = match self.peek()? {
                        Token::Ident(_) => {
                            if let Token::Ident(s) = self.next()? {
                                Some(s)
                            } else {
                                unreachable!()
                            }
                        }
                        _ => None,
                    };
                    let mut types = Vec::new();
                    while matches!(self.peek()?, Token::Colon) {
                        self.next()?;
                        types.push(self.expect_name()?);
                        // Support |TYPE2
                        while matches!(self.peek()?, Token::Pipe) {
                            self.next()?;
                            types.push(self.expect_name()?);
                        }
                    }
                    // Variable hops: *2..5 or * or *2
                    let (min_hops, max_hops) = if matches!(self.peek()?, Token::Star) {
                        self.next()?;
                        let min = if matches!(self.peek()?, Token::IntLit(_)) {
                            if let Token::IntLit(n) = self.next()? {
                                Some(n as u32)
                            } else {
                                unreachable!()
                            }
                        } else {
                            None
                        };
                        let max = if matches!(self.peek()?, Token::Dot) {
                            self.next()?; // first dot
                            if matches!(self.peek()?, Token::Dot) {
                                self.next()?; // second dot
                                if matches!(self.peek()?, Token::IntLit(_)) {
                                    if let Token::IntLit(n) = self.next()? {
                                        Some(n as u32)
                                    } else {
                                        unreachable!()
                                    }
                                } else {
                                    None
                                }
                            } else {
                                None
                            }
                        } else {
                            min
                        };
                        (min, max)
                    } else {
                        (None, None)
                    };
                    let props = if matches!(self.peek()?, Token::LBrace) {
                        self.parse_props_map()?
                    } else {
                        Vec::new()
                    };
                    self.expect(&Token::RBracket)?;
                    (var, types, props, min_hops, max_hops)
                } else {
                    (None, Vec::new(), Vec::new(), None, None)
                };

            // Trailing dashes and arrow
            let dir_right = if matches!(self.peek()?, Token::Dash | Token::DoubleDash) {
                self.next()?;
                if matches!(self.peek()?, Token::Gt) {
                    self.next()?;
                    true
                } else {
                    false
                }
            } else if matches!(self.peek()?, Token::Arrow) {
                self.next()?;
                true
            } else {
                false
            };

            let dir = match (dir_left, dir_right) {
                (true, false) => RelDir::Left,
                (false, true) => RelDir::Right,
                _ => RelDir::Both,
            };

            Ok(RelPattern {
                var,
                types,
                props,
                dir,
                min_hops,
                max_hops,
            })
        }

        fn parse_props_map(&mut self) -> Result<Vec<(String, Expr)>> {
            self.expect(&Token::LBrace)?;
            let mut props = Vec::new();
            if !matches!(self.peek()?, Token::RBrace) {
                loop {
                    let key = self.expect_name()?;
                    self.expect(&Token::Colon)?;
                    let val = self.parse_expr()?;
                    props.push((key, val));
                    if matches!(self.peek()?, Token::Comma) {
                        self.next()?;
                    } else {
                        break;
                    }
                }
            }
            self.expect(&Token::RBrace)?;
            Ok(props)
        }

        fn parse_return_items(&mut self) -> Result<Vec<ReturnItem>> {
            // Check for *
            if matches!(self.peek()?, Token::Star) {
                self.next()?;
                return Ok(vec![ReturnItem {
                    expr: Expr::Var("*".into()),
                    alias: None,
                }]);
            }
            let mut items = vec![self.parse_return_item()?];
            while matches!(self.peek()?, Token::Comma) {
                self.next()?;
                items.push(self.parse_return_item()?);
            }
            Ok(items)
        }

        fn parse_return_item(&mut self) -> Result<ReturnItem> {
            let expr = self.parse_expr()?;
            let alias = if matches!(self.peek()?, Token::As) {
                self.next()?;
                Some(self.expect_ident()?)
            } else {
                None
            };
            Ok(ReturnItem { expr, alias })
        }

        fn parse_set_items(&mut self) -> Result<Vec<SetItem>> {
            let mut items = vec![self.parse_set_item()?];
            while matches!(self.peek()?, Token::Comma) {
                self.next()?;
                items.push(self.parse_set_item()?);
            }
            Ok(items)
        }

        fn parse_set_item(&mut self) -> Result<SetItem> {
            let lhs = self.parse_postfix()?;
            match self.peek()? {
                Token::Eq | Token::PlusEq => {
                    self.next()?;
                    let rhs = self.parse_expr()?;
                    Ok(SetItem::PropSet(lhs, rhs))
                }
                Token::Colon => {
                    // label set: n:Label
                    if let Expr::Var(name) = lhs {
                        let mut labels = Vec::new();
                        while matches!(self.peek()?, Token::Colon) {
                            self.next()?;
                            labels.push(self.expect_ident()?);
                        }
                        Ok(SetItem::LabelSet(name, labels))
                    } else {
                        Err(CypherError::ParseError(
                            "expected variable in label SET".into(),
                        ))
                    }
                }
                tok => Err(CypherError::ParseError(format!(
                    "expected = or += in SET, got {:?}",
                    tok
                ))),
            }
        }

        // Expression parsing with precedence climbing
        // OR < AND < NOT < comparison < add/sub < mul/div < unary < postfix/atom

        pub fn parse_expr(&mut self) -> Result<Expr> {
            self.parse_or()
        }

        fn parse_or(&mut self) -> Result<Expr> {
            let mut lhs = self.parse_xor()?;
            loop {
                match self.peek()? {
                    Token::Or => {
                        self.next()?;
                        let rhs = self.parse_xor()?;
                        lhs = Expr::BinOp(BinOp::Or, Box::new(lhs), Box::new(rhs));
                    }
                    _ => break,
                }
            }
            Ok(lhs)
        }

        fn parse_xor(&mut self) -> Result<Expr> {
            let mut lhs = self.parse_and()?;
            loop {
                match self.peek()? {
                    Token::Xor => {
                        self.next()?;
                        let rhs = self.parse_and()?;
                        lhs = Expr::BinOp(BinOp::Xor, Box::new(lhs), Box::new(rhs));
                    }
                    _ => break,
                }
            }
            Ok(lhs)
        }

        fn parse_and(&mut self) -> Result<Expr> {
            let mut lhs = self.parse_not()?;
            loop {
                match self.peek()? {
                    Token::And => {
                        self.next()?;
                        let rhs = self.parse_not()?;
                        lhs = Expr::BinOp(BinOp::And, Box::new(lhs), Box::new(rhs));
                    }
                    _ => break,
                }
            }
            Ok(lhs)
        }

        fn parse_not(&mut self) -> Result<Expr> {
            if matches!(self.peek()?, Token::Not) {
                self.next()?;
                let inner = self.parse_not()?;
                Ok(Expr::UnOp(UnOp::Not, Box::new(inner)))
            } else {
                self.parse_comparison()
            }
        }

        fn parse_comparison(&mut self) -> Result<Expr> {
            let lhs = self.parse_concat()?;
            match self.peek()? {
                Token::Eq => {
                    self.next()?;
                    let rhs = self.parse_concat()?;
                    Ok(Expr::BinOp(BinOp::Eq, Box::new(lhs), Box::new(rhs)))
                }
                Token::Neq => {
                    self.next()?;
                    let rhs = self.parse_concat()?;
                    Ok(Expr::BinOp(BinOp::Neq, Box::new(lhs), Box::new(rhs)))
                }
                Token::Lt => {
                    self.next()?;
                    let rhs = self.parse_concat()?;
                    Ok(Expr::BinOp(BinOp::Lt, Box::new(lhs), Box::new(rhs)))
                }
                Token::Lte => {
                    self.next()?;
                    let rhs = self.parse_concat()?;
                    Ok(Expr::BinOp(BinOp::Lte, Box::new(lhs), Box::new(rhs)))
                }
                Token::Gt => {
                    self.next()?;
                    let rhs = self.parse_concat()?;
                    Ok(Expr::BinOp(BinOp::Gt, Box::new(lhs), Box::new(rhs)))
                }
                Token::Gte => {
                    self.next()?;
                    let rhs = self.parse_concat()?;
                    Ok(Expr::BinOp(BinOp::Gte, Box::new(lhs), Box::new(rhs)))
                }
                Token::In => {
                    self.next()?;
                    let rhs = self.parse_concat()?;
                    Ok(Expr::In(Box::new(lhs), Box::new(rhs)))
                }
                Token::Is => {
                    self.next()?;
                    if matches!(self.peek()?, Token::Not) {
                        self.next()?;
                        self.expect(&Token::Null)?;
                        Ok(Expr::IsNotNull(Box::new(lhs)))
                    } else {
                        self.expect(&Token::Null)?;
                        Ok(Expr::IsNull(Box::new(lhs)))
                    }
                }
                Token::RegexMatch => {
                    self.next()?;
                    let rhs = self.parse_concat()?;
                    Ok(Expr::RegexMatch(Box::new(lhs), Box::new(rhs)))
                }
                Token::Ident(s) if s.eq_ignore_ascii_case("starts") => {
                    self.next()?;
                    self.expect(&Token::With)?;
                    let rhs = self.parse_concat()?;
                    Ok(Expr::StartsWith(Box::new(lhs), Box::new(rhs)))
                }
                Token::Ident(s) if s.eq_ignore_ascii_case("ends") => {
                    self.next()?;
                    self.expect(&Token::With)?;
                    let rhs = self.parse_concat()?;
                    Ok(Expr::EndsWith(Box::new(lhs), Box::new(rhs)))
                }
                Token::Ident(s) if s.eq_ignore_ascii_case("contains") => {
                    self.next()?;
                    let rhs = self.parse_concat()?;
                    Ok(Expr::Contains(Box::new(lhs), Box::new(rhs)))
                }
                _ => Ok(lhs),
            }
        }

        fn parse_concat(&mut self) -> Result<Expr> {
            let lhs = self.parse_add_sub()?;
            Ok(lhs)
        }

        fn parse_add_sub(&mut self) -> Result<Expr> {
            let mut lhs = self.parse_mul_div()?;
            loop {
                match self.peek()? {
                    Token::Plus => {
                        self.next()?;
                        let rhs = self.parse_mul_div()?;
                        lhs = Expr::BinOp(BinOp::Add, Box::new(lhs), Box::new(rhs));
                    }
                    Token::Minus | Token::Dash => {
                        self.next()?;
                        let rhs = self.parse_mul_div()?;
                        lhs = Expr::BinOp(BinOp::Sub, Box::new(lhs), Box::new(rhs));
                    }
                    _ => break,
                }
            }
            Ok(lhs)
        }

        fn parse_mul_div(&mut self) -> Result<Expr> {
            let mut lhs = self.parse_unary()?;
            loop {
                match self.peek()? {
                    Token::Star => {
                        self.next()?;
                        let rhs = self.parse_unary()?;
                        lhs = Expr::BinOp(BinOp::Mul, Box::new(lhs), Box::new(rhs));
                    }
                    Token::Slash => {
                        self.next()?;
                        let rhs = self.parse_unary()?;
                        lhs = Expr::BinOp(BinOp::Div, Box::new(lhs), Box::new(rhs));
                    }
                    Token::Percent => {
                        self.next()?;
                        let rhs = self.parse_unary()?;
                        lhs = Expr::BinOp(BinOp::Mod, Box::new(lhs), Box::new(rhs));
                    }
                    _ => break,
                }
            }
            Ok(lhs)
        }

        fn parse_unary(&mut self) -> Result<Expr> {
            match self.peek()? {
                Token::Minus | Token::Dash => {
                    self.next()?;
                    let inner = self.parse_unary()?;
                    Ok(Expr::UnOp(UnOp::Neg, Box::new(inner)))
                }
                Token::Not => {
                    self.next()?;
                    let inner = self.parse_unary()?;
                    Ok(Expr::UnOp(UnOp::Not, Box::new(inner)))
                }
                _ => self.parse_postfix(),
            }
        }

        fn parse_postfix(&mut self) -> Result<Expr> {
            let mut expr = self.parse_atom()?;
            loop {
                match self.peek()? {
                    Token::Dot => {
                        self.next()?;
                        let prop = self.expect_ident()?;
                        expr = Expr::Prop(Box::new(expr), prop);
                    }
                    Token::LBracket => {
                        self.next()?; // consume [
                        let idx = self.parse_expr()?;
                        self.expect(&Token::RBracket)?;
                        expr = Expr::Index(Box::new(expr), Box::new(idx));
                    }
                    Token::LBrace => {
                        // Map projection: n { .name, .age, key: expr, .* }
                        // Only if next token after { is . or .* (to distinguish from map literal)
                        let saved_pos = self.lexer.pos;
                        let saved_peeked = self.lexer.peeked.clone();
                        self.next()?; // consume {
                        if matches!(self.peek()?, Token::Dot | Token::RBrace) || self.is_map_proj_start()? {
                            let entries = self.parse_map_proj_entries()?;
                            self.expect(&Token::RBrace)?;
                            expr = Expr::MapProjection {
                                base: Box::new(expr),
                                entries,
                            };
                        } else {
                            // Not map projection, restore
                            self.lexer.pos = saved_pos;
                            self.lexer.peeked = saved_peeked;
                            break;
                        }
                    }
                    _ => break,
                }
            }
            Ok(expr)
        }

        fn is_map_proj_start(&mut self) -> Result<bool> {
            // Check if current context looks like a map projection entry
            // (key: expr, ...) — but this is already handled by checking for Dot
            Ok(false)
        }

        fn parse_map_proj_entries(&mut self) -> Result<Vec<MapProjEntry>> {
            let mut entries = Vec::new();
            if matches!(self.peek()?, Token::RBrace) {
                return Ok(entries);
            }
            loop {
                if matches!(self.peek()?, Token::Dot) {
                    self.next()?; // consume .
                    if matches!(self.peek()?, Token::Star) {
                        self.next()?; // consume *
                        entries.push(MapProjEntry::AllProps);
                    } else {
                        let name = self.expect_ident()?;
                        entries.push(MapProjEntry::Prop(name));
                    }
                } else {
                    // key: expr
                    let key = self.expect_name()?;
                    self.expect(&Token::Colon)?;
                    let val = self.parse_expr()?;
                    entries.push(MapProjEntry::Literal(key, val));
                }
                if matches!(self.peek()?, Token::Comma) {
                    self.next()?;
                } else {
                    break;
                }
            }
            Ok(entries)
        }

        fn parse_atom(&mut self) -> Result<Expr> {
            match self.peek()? {
                Token::IntLit(_) => {
                    if let Token::IntLit(n) = self.next()? {
                        Ok(Expr::Lit(Literal::Int(n)))
                    } else {
                        unreachable!()
                    }
                }
                Token::FloatLit(_) => {
                    if let Token::FloatLit(f) = self.next()? {
                        Ok(Expr::Lit(Literal::Float(f)))
                    } else {
                        unreachable!()
                    }
                }
                Token::StrLit(_) => {
                    if let Token::StrLit(s) = self.next()? {
                        Ok(Expr::Lit(Literal::Str(s)))
                    } else {
                        unreachable!()
                    }
                }
                Token::True => {
                    self.next()?;
                    Ok(Expr::Lit(Literal::Bool(true)))
                }
                Token::False => {
                    self.next()?;
                    Ok(Expr::Lit(Literal::Bool(false)))
                }
                Token::Null => {
                    self.next()?;
                    Ok(Expr::Lit(Literal::Null))
                }
                Token::Param(_) => {
                    if let Token::Param(name) = self.next()? {
                        Ok(Expr::Param(name))
                    } else {
                        unreachable!()
                    }
                }
                Token::LParen => {
                    self.next()?;
                    let expr = self.parse_expr()?;
                    self.expect(&Token::RParen)?;
                    Ok(expr)
                }
                Token::LBracket => {
                    self.next()?;
                    // Check for list comprehension: [x IN list WHERE pred | expr]
                    if let Token::Ident(_) = self.peek()?.clone() {
                        // Speculatively check for list comp pattern: ident IN ...
                        let saved_pos = self.lexer.pos;
                        let saved_peeked = self.lexer.peeked.clone();
                        if let Token::Ident(var) = self.next()? {
                            if matches!(self.peek()?, Token::In) {
                                self.next()?; // consume IN
                                let list_expr = self.parse_expr()?;
                                let filter = if matches!(self.peek()?, Token::Where) {
                                    self.next()?;
                                    Some(Box::new(self.parse_expr()?))
                                } else {
                                    None
                                };
                                let map_expr = if matches!(self.peek()?, Token::Pipe) {
                                    self.next()?;
                                    Some(Box::new(self.parse_expr()?))
                                } else {
                                    None
                                };
                                self.expect(&Token::RBracket)?;
                                return Ok(Expr::ListComp {
                                    var,
                                    list_expr: Box::new(list_expr),
                                    filter,
                                    map_expr,
                                });
                            } else {
                                // Not a list comprehension, restore state
                                self.lexer.pos = saved_pos;
                                self.lexer.peeked = saved_peeked;
                            }
                        }
                    }
                    let mut items = Vec::new();
                    if !matches!(self.peek()?, Token::RBracket) {
                        items.push(self.parse_expr()?);
                        while matches!(self.peek()?, Token::Comma) {
                            self.next()?;
                            items.push(self.parse_expr()?);
                        }
                    }
                    self.expect(&Token::RBracket)?;
                    Ok(Expr::List(items))
                }
                Token::LBrace => {
                    self.next()?;
                    let mut entries = Vec::new();
                    if !matches!(self.peek()?, Token::RBrace) {
                        loop {
                            let key = self.expect_name()?;
                            self.expect(&Token::Colon)?;
                            let val = self.parse_expr()?;
                            entries.push((key, val));
                            if matches!(self.peek()?, Token::Comma) {
                                self.next()?;
                            } else {
                                break;
                            }
                        }
                    }
                    self.expect(&Token::RBrace)?;
                    Ok(Expr::Map(entries))
                }
                Token::Case => {
                    self.next()?;
                    self.parse_case()
                }
                Token::Exists => {
                    self.next()?; // consume EXISTS
                    if matches!(self.peek()?, Token::LBrace) {
                        // EXISTS { subquery }
                        self.next()?; // consume {
                        let mut subquery = Vec::new();
                        while !matches!(self.peek()?, Token::RBrace) {
                            subquery.push(self.parse_clause()?);
                        }
                        self.expect(&Token::RBrace)?;
                        Ok(Expr::ExistsSubquery(subquery))
                    } else if matches!(self.peek()?, Token::LParen) {
                        // exists(expr) — function form
                        self.next()?; // consume (
                        let mut args = Vec::new();
                        if !matches!(self.peek()?, Token::RParen) {
                            args.push(self.parse_expr()?);
                        }
                        self.expect(&Token::RParen)?;
                        Ok(Expr::FnCall("exists".into(), args))
                    } else {
                        Err(CypherError::ParseError("expected { or ( after EXISTS".into()))
                    }
                }
                Token::Ident(_) => {
                    if let Token::Ident(name) = self.next()? {
                        let name_lower = name.to_ascii_lowercase();
                        // Check for list predicate: ALL/ANY/NONE/SINGLE(var IN list WHERE pred)
                        let pred_kind = match name_lower.as_str() {
                            "all" => Some(ListPredicateKind::All),
                            "any" => Some(ListPredicateKind::Any),
                            "none" => Some(ListPredicateKind::None),
                            "single" => Some(ListPredicateKind::Single),
                            _ => None,
                        };
                        // Function call?
                        if matches!(self.peek()?, Token::LParen) {
                            self.next()?; // consume (
                            // Try list predicate pattern: kind(var IN list WHERE pred)
                            if pred_kind.is_some() {
                                if let Token::Ident(_) = self.peek()?.clone() {
                                    let saved_pos = self.lexer.pos;
                                    let saved_peeked = self.lexer.peeked.clone();
                                    if let Token::Ident(var) = self.next()? {
                                        if matches!(self.peek()?, Token::In) {
                                            self.next()?; // consume IN
                                            let list_expr = self.parse_expr()?;
                                            self.expect(&Token::Where)?;
                                            let pred = self.parse_expr()?;
                                            self.expect(&Token::RParen)?;
                                            return Ok(Expr::ListPredicate {
                                                kind: pred_kind.unwrap(),
                                                var,
                                                list_expr: Box::new(list_expr),
                                                pred: Box::new(pred),
                                            });
                                        } else {
                                            // Not list predicate, restore
                                            self.lexer.pos = saved_pos;
                                            self.lexer.peeked = saved_peeked;
                                        }
                                    }
                                }
                            }
                            let mut args = Vec::new();
                            if !matches!(self.peek()?, Token::RParen) {
                                // Handle count(*)
                                if matches!(self.peek()?, Token::Star) {
                                    self.next()?;
                                    args.push(Expr::Var("*".into()));
                                } else if name_lower == "count" && matches!(self.peek()?, Token::Distinct) {
                                    // count(DISTINCT expr)
                                    self.next()?; // consume DISTINCT
                                    args.push(self.parse_expr()?);
                                    while matches!(self.peek()?, Token::Comma) {
                                        self.next()?;
                                        args.push(self.parse_expr()?);
                                    }
                                    self.expect(&Token::RParen)?;
                                    return Ok(Expr::FnCall("count_distinct".into(), args));
                                } else {
                                    args.push(self.parse_expr()?);
                                    while matches!(self.peek()?, Token::Comma) {
                                        self.next()?;
                                        args.push(self.parse_expr()?);
                                    }
                                }
                            }
                            self.expect(&Token::RParen)?;
                            Ok(Expr::FnCall(name_lower, args))
                        } else {
                            Ok(Expr::Var(name))
                        }
                    } else {
                        unreachable!()
                    }
                }
                tok => Err(CypherError::ParseError(format!(
                    "unexpected token in expression: {:?}",
                    tok
                ))),
            }
        }

        fn parse_case(&mut self) -> Result<Expr> {
            // CASE [expr] WHEN ... THEN ... [ELSE ...] END
            let scrutinee = if !matches!(self.peek()?, Token::When) {
                Some(Box::new(self.parse_expr()?))
            } else {
                None
            };
            let mut whens = Vec::new();
            while matches!(self.peek()?, Token::When) {
                self.next()?;
                let cond = self.parse_expr()?;
                match self.next()? {
                    Token::Then => {}
                    tok => {
                        return Err(CypherError::ParseError(format!(
                            "expected THEN in CASE, got {:?}",
                            tok
                        )))
                    }
                }
                let result = self.parse_expr()?;
                whens.push((cond, result));
            }
            let else_ = if matches!(self.peek()?, Token::Else) {
                self.next()?;
                Some(Box::new(self.parse_expr()?))
            } else {
                None
            };
            match self.next()? {
                Token::End => {}
                tok => {
                    return Err(CypherError::ParseError(format!(
                        "expected END in CASE, got {:?}",
                        tok
                    )))
                }
            }
            Ok(Expr::Case {
                scrutinee,
                whens,
                else_,
            })
        }
    }
}

// ---- graph --------------------------------------------------------------

pub mod graph {
    use crate::types::{NodeRef, RelRef, Value};
    use indexmap::IndexMap;

    pub trait Graph: Send + Sync {
        fn nodes(&self) -> Vec<NodeRef>;
        fn rels(&self) -> Vec<RelRef>;
        fn node_by_id(&self, id: &str) -> Option<NodeRef>;
        fn rel_by_id(&self, id: &str) -> Option<RelRef>;
        fn rels_from(&self, node_id: &str) -> Vec<RelRef>;
        fn rels_to(&self, node_id: &str) -> Vec<RelRef>;
        fn add_node(&mut self, node: NodeRef);
        fn add_rel(&mut self, rel: RelRef);
        fn set_node_prop(&mut self, id: &str, key: &str, val: Value);
        fn set_node_labels(&mut self, id: &str, labels: &[String]) {
            let _ = (id, labels);
        }
        fn delete_node(&mut self, id: &str);
        fn delete_rel(&mut self, id: &str);
        /// Build adjacency index for O(degree) neighbor lookup. Default no-op.
        fn ensure_adjacency_index(&mut self) {}
        /// Vector search: return nodes matching labels sorted by similarity (descending).
        fn vector_search(&self, _query: &[f32], _limit: usize, _labels: &[String]) -> Vec<(NodeRef, f64)> {
            vec![]
        }
        /// Store embedding vector for a node.
        fn set_node_embedding(&mut self, _id: &str, _embedding: &[f32]) {
            // default no-op
        }
    }

    #[derive(Clone)]
    pub struct MemoryGraph {
        nodes: IndexMap<String, NodeRef>,
        rels: IndexMap<String, RelRef>,
        /// CSR adjacency index — built lazily, invalidated on mutation.
        csr: Option<crate::csr::CsrIndex>,
    }

    impl Default for MemoryGraph {
        fn default() -> Self {
            Self { nodes: IndexMap::new(), rels: IndexMap::new(), csr: None }
        }
    }

    impl MemoryGraph {
        pub fn new() -> Self {
            Self::default()
        }

        /// Build (or rebuild) the CSR adjacency index from current edges.
        pub fn build_csr(&mut self) {
            let edges: Vec<(String, String, String)> = self.rels.values()
                .map(|r| (r.src.clone(), r.dst.clone(), r.id.clone()))
                .collect();
            self.csr = Some(crate::csr::CsrIndex::build(&edges));
        }

        /// Ensure CSR is built. No-op if already built.
        pub fn ensure_csr(&mut self) {
            if self.csr.is_none() {
                self.build_csr();
            }
        }

        /// Invalidate CSR (call after any mutation).
        fn invalidate_csr(&mut self) {
            self.csr = None;
        }

        /// Retain only nodes for which the predicate returns true.
        pub fn retain_nodes(&mut self, f: impl Fn(&NodeRef) -> bool) {
            self.nodes.retain(|_, n| f(n));
            self.invalidate_csr();
        }

        /// Retain only rels for which the predicate returns true.
        pub fn retain_rels(&mut self, f: impl Fn(&RelRef) -> bool) {
            self.rels.retain(|_, r| f(r));
            self.invalidate_csr();
        }

        pub fn from_ocel(
            objects: &[yata_ocel::OcelObject],
            edges: &[yata_ocel::OcelObjectObjectEdge],
        ) -> Self {
            let mut g = Self::new();
            for obj in objects {
                let mut props = IndexMap::new();
                for (k, v) in &obj.attrs {
                    props.insert(k.clone(), json_to_value(v));
                }
                g.add_node(NodeRef {
                    id: obj.object_id.clone(),
                    labels: vec![obj.object_type.clone()],
                    props,
                });
            }
            for edge in edges {
                let id = uuid::Uuid::new_v4().to_string();
                let mut props = IndexMap::new();
                if let Some(q) = &edge.qualifier {
                    props.insert("qualifier".into(), Value::Str(q.clone()));
                }
                g.add_rel(RelRef {
                    id,
                    rel_type: edge.rel_type.clone(),
                    src: edge.src_object_id.clone(),
                    dst: edge.dst_object_id.clone(),
                    props,
                });
            }
            g
        }
    }

    fn json_to_value(v: &serde_json::Value) -> Value {
        match v {
            serde_json::Value::Null => Value::Null,
            serde_json::Value::Bool(b) => Value::Bool(*b),
            serde_json::Value::Number(n) => {
                if let Some(i) = n.as_i64() {
                    Value::Int(i)
                } else {
                    Value::Float(n.as_f64().unwrap_or(0.0))
                }
            }
            serde_json::Value::String(s) => Value::Str(s.clone()),
            serde_json::Value::Array(arr) => {
                Value::List(arr.iter().map(json_to_value).collect())
            }
            serde_json::Value::Object(obj) => {
                let mut m = IndexMap::new();
                for (k, v) in obj {
                    m.insert(k.clone(), json_to_value(v));
                }
                Value::Map(m)
            }
        }
    }

    impl Graph for MemoryGraph {
        fn nodes(&self) -> Vec<NodeRef> {
            self.nodes.values().cloned().collect()
        }

        fn rels(&self) -> Vec<RelRef> {
            self.rels.values().cloned().collect()
        }

        fn node_by_id(&self, id: &str) -> Option<NodeRef> {
            self.nodes.get(id).cloned()
        }

        fn rel_by_id(&self, id: &str) -> Option<RelRef> {
            self.rels.get(id).cloned()
        }

        fn rels_from(&self, node_id: &str) -> Vec<RelRef> {
            if let Some(csr) = &self.csr {
                let rel_values: Vec<&RelRef> = self.rels.values().collect();
                csr.out_edge_indices(node_id)
                    .iter()
                    .filter_map(|&idx| rel_values.get(idx).map(|r| (*r).clone()))
                    .collect()
            } else {
                self.rels
                    .values()
                    .filter(|r| r.src == node_id)
                    .cloned()
                    .collect()
            }
        }

        fn rels_to(&self, node_id: &str) -> Vec<RelRef> {
            if let Some(csr) = &self.csr {
                let rel_values: Vec<&RelRef> = self.rels.values().collect();
                csr.in_edge_indices(node_id)
                    .iter()
                    .filter_map(|&idx| rel_values.get(idx).map(|r| (*r).clone()))
                    .collect()
            } else {
                self.rels
                    .values()
                    .filter(|r| r.dst == node_id)
                    .cloned()
                    .collect()
            }
        }

        fn add_node(&mut self, node: NodeRef) {
            self.nodes.insert(node.id.clone(), node);
        }

        fn add_rel(&mut self, rel: RelRef) {
            self.rels.insert(rel.id.clone(), rel);
            self.invalidate_csr();
        }

        fn set_node_prop(&mut self, id: &str, key: &str, val: Value) {
            if let Some(node) = self.nodes.get_mut(id) {
                node.props.insert(key.to_owned(), val);
            }
        }

        fn set_node_labels(&mut self, id: &str, labels: &[String]) {
            if let Some(node) = self.nodes.get_mut(id) {
                for l in labels {
                    if !node.labels.contains(l) {
                        node.labels.push(l.clone());
                    }
                }
            }
        }

        fn delete_node(&mut self, id: &str) {
            self.nodes.shift_remove(id);
            self.invalidate_csr();
        }

        fn delete_rel(&mut self, id: &str) {
            self.rels.shift_remove(id);
            self.invalidate_csr();
        }

        fn ensure_adjacency_index(&mut self) {
            self.ensure_csr();
        }

        fn vector_search(&self, query: &[f32], limit: usize, labels: &[String]) -> Vec<(NodeRef, f64)> {
            let mut scored: Vec<(NodeRef, f64)> = self.nodes.values()
                .filter(|n| labels.is_empty() || labels.iter().any(|l| n.labels.contains(l)))
                .filter_map(|n| {
                    let embedding = extract_f32_vec(n.props.get("embedding")?)?;
                    let score = cosine_similarity(query, &embedding);
                    Some((n.clone(), score))
                })
                .collect();
            scored.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
            scored.truncate(limit);
            scored
        }

        fn set_node_embedding(&mut self, id: &str, embedding: &[f32]) {
            if let Some(node) = self.nodes.get_mut(id) {
                node.props.insert(
                    "embedding".into(),
                    Value::List(embedding.iter().map(|v| Value::Float(*v as f64)).collect()),
                );
            }
        }
    }

    pub fn extract_f32_vec(val: &Value) -> Option<Vec<f32>> {
        if let Value::List(items) = val {
            items.iter().map(|v| match v {
                Value::Float(f) => Some(*f as f32),
                Value::Int(i) => Some(*i as f32),
                _ => None,
            }).collect()
        } else {
            None
        }
    }

    pub fn cosine_similarity(a: &[f32], b: &[f32]) -> f64 {
        if a.len() != b.len() || a.is_empty() { return 0.0; }
        let dot: f64 = a.iter().zip(b).map(|(x, y)| (*x as f64) * (*y as f64)).sum();
        let norm_a: f64 = a.iter().map(|x| (*x as f64).powi(2)).sum::<f64>().sqrt();
        let norm_b: f64 = b.iter().map(|x| (*x as f64).powi(2)).sum::<f64>().sqrt();
        if norm_a == 0.0 || norm_b == 0.0 { return 0.0; }
        dot / (norm_a * norm_b)
    }

    pub fn euclidean_distance(a: &[f32], b: &[f32]) -> f64 {
        if a.len() != b.len() { return f64::MAX; }
        a.iter().zip(b).map(|(x, y)| ((*x as f64) - (*y as f64)).powi(2)).sum::<f64>().sqrt()
    }
}

// ---- executor -----------------------------------------------------------

pub mod executor {
    use crate::ast::*;
    use crate::error::{CypherError, Result};
    use crate::graph::Graph;
    use crate::types::{NodeRef, RelRef, ResultSet, Row, Value};
    use indexmap::IndexMap;

    type Binding = IndexMap<String, Value>;

    pub struct Executor {
        params: IndexMap<String, Value>,
    }

    impl Executor {
        pub fn new() -> Self {
            Self {
                params: IndexMap::new(),
            }
        }

        pub fn with_params(params: IndexMap<String, Value>) -> Self {
            Self { params }
        }

        pub fn execute(&self, query: &Query, graph: &mut dyn Graph) -> Result<ResultSet> {
            // Build CSR adjacency index for O(degree) neighbor lookup during MATCH.
            graph.ensure_adjacency_index();

            // Split clauses at UNION boundaries and execute each sub-query
            let mut segments: Vec<(Vec<&Clause>, bool)> = Vec::new(); // (clauses, union_all)
            let mut current_segment: Vec<&Clause> = Vec::new();

            for clause in &query.clauses {
                if let Clause::Union { all } = clause {
                    segments.push((std::mem::take(&mut current_segment), *all));
                } else {
                    current_segment.push(clause);
                }
            }
            // Last segment (always present)
            if !current_segment.is_empty() || segments.is_empty() {
                segments.push((current_segment, false));
            }

            if segments.len() == 1 {
                // No UNION — fast path
                return self.execute_clauses(&segments[0].0, graph);
            }

            // Execute each segment and merge results
            let mut combined = ResultSet::empty();
            for (i, (clauses, _)) in segments.iter().enumerate() {
                let seg_result = self.execute_clauses(clauses, graph)?;
                if i == 0 {
                    combined = seg_result;
                } else {
                    // Merge columns (should be same)
                    if combined.columns.is_empty() {
                        combined.columns = seg_result.columns;
                    }
                    combined.rows.extend(seg_result.rows);
                }
            }

            // Deduplicate if any UNION (not ALL)
            let has_non_all = segments.iter().any(|(_, all)| !*all);
            if has_non_all && segments.len() > 1 {
                let deduped = self.dedup_rows(&combined.rows);
                combined.rows = deduped;
            }

            Ok(combined)
        }

        fn execute_clauses(&self, clauses: &[&Clause], graph: &mut dyn Graph) -> Result<ResultSet> {
            let mut bindings: Vec<Binding> = vec![IndexMap::new()];
            let mut result = ResultSet::empty();

            for clause in clauses {
                match clause {
                    Clause::Match { patterns, where_ } => {
                        bindings = self.execute_match(patterns, where_, &bindings, graph, false)?;
                    }
                    Clause::OptionalMatch { patterns, where_ } => {
                        bindings =
                            self.execute_match(patterns, where_, &bindings, graph, true)?;
                    }
                    Clause::Create { patterns } => {
                        self.execute_create(patterns, &mut bindings, graph)?;
                    }
                    Clause::Merge { pattern, on_create, on_match } => {
                        self.execute_merge(pattern, on_create, on_match, &mut bindings, graph)?;
                    }
                    Clause::Return {
                        items,
                        distinct,
                        order_by,
                        limit,
                        skip,
                    } => {
                        result = self.execute_return(
                            items, *distinct, order_by, limit, skip, &bindings,
                        )?;
                    }
                    Clause::With { items, where_ } => {
                        bindings = self.execute_with(items, where_, &bindings)?;
                    }
                    Clause::Set { items } => {
                        self.execute_set(items, &bindings, graph)?;
                        // Refresh bindings from graph after SET so RETURN sees updated props
                        for binding in &mut bindings {
                            let keys: Vec<String> = binding.keys().cloned().collect();
                            for key in keys {
                                if let Some(Value::Node(n)) = binding.get(&key) {
                                    if let Some(updated) = graph.node_by_id(&n.id) {
                                        binding.insert(key, Value::Node(updated));
                                    }
                                }
                            }
                        }
                    }
                    Clause::Delete { exprs, detach } => {
                        self.execute_delete(exprs, *detach, &bindings, graph)?;
                    }
                    Clause::Unwind { expr, alias } => {
                        bindings = self.execute_unwind(expr, alias, &bindings)?;
                    }
                    Clause::Union { .. } => {
                        // Handled at top level
                    }
                    Clause::Foreach { var, list_expr, body } => {
                        self.execute_foreach(var, list_expr, body, &bindings, graph)?;
                    }
                    Clause::Remove { items } => {
                        self.execute_remove(items, &bindings, graph)?;
                    }
                    Clause::Call { subquery } => {
                        bindings = self.execute_call(subquery, &bindings, graph)?;
                    }
                    Clause::CallProcedure { name, args, yields } => {
                        bindings = self.execute_call_procedure(name, args, yields, &bindings, graph)?;
                    }
                }
            }

            Ok(result)
        }

        fn dedup_rows(&self, rows: &[Row]) -> Vec<Row> {
            let mut seen: Vec<Vec<(String, String)>> = Vec::new();
            let mut result = Vec::new();
            for row in rows {
                let key: Vec<(String, String)> =
                    row.0.iter().map(|(k, v)| (k.clone(), v.to_string())).collect();
                if !seen.contains(&key) {
                    seen.push(key);
                    result.push(row.clone());
                }
            }
            result
        }

        // ---- MATCH ----------------------------------------------------------

        fn execute_match(
            &self,
            patterns: &[Pattern],
            where_: &Option<Expr>,
            input: &[Binding],
            graph: &dyn Graph,
            optional: bool,
        ) -> Result<Vec<Binding>> {
            let mut result = Vec::new();

            for binding in input {
                let mut pattern_bindings = vec![binding.clone()];

                for pattern in patterns {
                    let mut next = Vec::new();
                    for b in &pattern_bindings {
                        let matched = self.match_pattern(pattern, b, graph)?;
                        next.extend(matched);
                    }
                    pattern_bindings = next;
                }

                // Apply WHERE (use eval_with_graph if EXISTS subqueries present)
                if let Some(where_expr) = where_ {
                    let has_exists = self.contains_exists(where_expr);
                    let mut filtered = Vec::new();
                    for b in pattern_bindings {
                        let matches = if has_exists {
                            self.eval_with_graph(where_expr, &b, graph)?.is_truthy()
                        } else {
                            self.eval(where_expr, &b)?.is_truthy()
                        };
                        if matches {
                            filtered.push(b);
                        }
                    }
                    pattern_bindings = filtered;
                }

                if pattern_bindings.is_empty() && optional {
                    result.push(binding.clone());
                } else {
                    result.extend(pattern_bindings);
                }
            }

            Ok(result)
        }

        fn match_pattern(
            &self,
            pattern: &Pattern,
            binding: &Binding,
            graph: &dyn Graph,
        ) -> Result<Vec<Binding>> {
            // Elements alternate: Node, Rel, Node, Rel, Node ...
            let elements = &pattern.elements;
            if elements.is_empty() {
                return Ok(vec![binding.clone()]);
            }

            let mut current_bindings = vec![binding.clone()];

            let mut i = 0;
            while i < elements.len() {
                match &elements[i] {
                    PatternElement::Node(np) => {
                        let mut next = Vec::new();
                        for b in &current_bindings {
                            let candidates = self.candidates_for_node(np, b, graph);
                            for node in candidates {
                                if self.node_matches(np, &node, b)? {
                                    let mut nb = b.clone();
                                    if let Some(var) = &np.var {
                                        if var != "_" {
                                            nb.insert(var.clone(), Value::Node(node));
                                        }
                                    }
                                    next.push(nb);
                                }
                            }
                        }
                        current_bindings = next;
                        i += 1;
                    }
                    PatternElement::Rel(rp) => {
                        // Must be followed by a node
                        let next_np = if i + 1 < elements.len() {
                            match &elements[i + 1] {
                                PatternElement::Node(np) => np,
                                _ => {
                                    return Err(CypherError::ParseError(
                                        "expected node after relationship in pattern".into(),
                                    ))
                                }
                            }
                        } else {
                            return Err(CypherError::ParseError(
                                "dangling relationship in pattern".into(),
                            ));
                        };

                        let mut next = Vec::new();
                        for b in &current_bindings {
                            // Get the last bound node (the src side)
                            let src_node = self.get_last_node_in_binding(b, elements, i)?;
                            let expanded =
                                self.match_rel_step(rp, next_np, &src_node, b, graph)?;
                            next.extend(expanded);
                        }
                        current_bindings = next;
                        i += 2; // skip rel + next node
                    }
                }
            }

            // Bind named path variable if present
            if let Some(path_var) = &pattern.path_var {
                for b in &mut current_bindings {
                    let path = self.build_path_value(elements, b);
                    b.insert(path_var.clone(), path);
                }
            }

            Ok(current_bindings)
        }

        /// Build a path value (list of alternating nodes and rels) from bound pattern elements
        fn build_path_value(&self, elements: &[PatternElement], binding: &Binding) -> Value {
            let mut path = Vec::new();
            for elem in elements {
                match elem {
                    PatternElement::Node(np) => {
                        if let Some(var) = &np.var {
                            if let Some(val) = binding.get(var) {
                                path.push(val.clone());
                            }
                        }
                    }
                    PatternElement::Rel(rp) => {
                        if let Some(var) = &rp.var {
                            if let Some(val) = binding.get(var) {
                                path.push(val.clone());
                            }
                        }
                    }
                }
            }
            Value::List(path)
        }

        fn get_last_node_in_binding(
            &self,
            binding: &Binding,
            elements: &[PatternElement],
            rel_idx: usize,
        ) -> Result<NodeRef> {
            // The node just before the rel at rel_idx
            if rel_idx == 0 {
                return Err(CypherError::ParseError(
                    "relationship without preceding node".into(),
                ));
            }
            match &elements[rel_idx - 1] {
                PatternElement::Node(np) => {
                    if let Some(var) = &np.var {
                        match binding.get(var) {
                            Some(Value::Node(n)) => Ok(n.clone()),
                            _ => Err(CypherError::UnboundVariable(var.clone())),
                        }
                    } else {
                        // Anonymous node — must have been added without a var
                        // We need to carry it in a temporary slot
                        Err(CypherError::ParseError(
                            "anonymous src node in multi-hop pattern requires variable".into(),
                        ))
                    }
                }
                _ => Err(CypherError::ParseError(
                    "expected node before relationship".into(),
                )),
            }
        }

        fn candidates_for_node(
            &self,
            np: &NodePattern,
            binding: &Binding,
            graph: &dyn Graph,
        ) -> Vec<NodeRef> {
            if let Some(var) = &np.var {
                if let Some(Value::Node(n)) = binding.get(var) {
                    return vec![n.clone()];
                }
            }
            graph.nodes()
        }

        fn node_matches(
            &self,
            np: &NodePattern,
            node: &NodeRef,
            binding: &Binding,
        ) -> Result<bool> {
            // Check if var already bound to a different node
            if let Some(var) = &np.var {
                if let Some(Value::Node(existing)) = binding.get(var) {
                    if existing.id != node.id {
                        return Ok(false);
                    }
                }
            }
            // Check labels
            for label in &np.labels {
                if !node.labels.contains(label) {
                    return Ok(false);
                }
            }
            // Check props
            for (key, expr) in &np.props {
                let expected = self.eval(expr, binding)?;
                match node.props.get(key) {
                    Some(actual) => {
                        if !actual.eq_val(&expected) {
                            return Ok(false);
                        }
                    }
                    None => return Ok(false),
                }
            }
            Ok(true)
        }

        fn match_rel_step(
            &self,
            rp: &RelPattern,
            next_np: &NodePattern,
            src_node: &NodeRef,
            binding: &Binding,
            graph: &dyn Graph,
        ) -> Result<Vec<Binding>> {
            // Variable-hop: dispatch to BFS traversal
            if rp.min_hops.is_some() || rp.max_hops.is_some() {
                return self.match_variable_hop(rp, next_np, src_node, binding, graph);
            }

            let mut result = Vec::new();

            // Determine candidate rels based on direction
            let candidates: Vec<RelRef> = match rp.dir {
                RelDir::Right | RelDir::Both => {
                    let mut v = graph.rels_from(&src_node.id);
                    if matches!(rp.dir, RelDir::Both) {
                        v.extend(graph.rels_to(&src_node.id));
                    }
                    v
                }
                RelDir::Left => graph.rels_to(&src_node.id),
            };

            for rel in candidates {
                // Check rel type
                if !rp.types.is_empty() {
                    if !rp.types.contains(&rel.rel_type) {
                        continue;
                    }
                }
                // Check rel props
                let mut rel_ok = true;
                for (key, expr) in &rp.props {
                    let expected = self.eval(expr, binding)?;
                    match rel.props.get(key) {
                        Some(actual) if actual.eq_val(&expected) => {}
                        _ => {
                            rel_ok = false;
                            break;
                        }
                    }
                }
                if !rel_ok {
                    continue;
                }

                // Check if rel var already bound
                if let Some(rv) = &rp.var {
                    if let Some(Value::Rel(existing)) = binding.get(rv) {
                        if existing.id != rel.id {
                            continue;
                        }
                    }
                }

                // Determine dst node id
                let dst_id = match rp.dir {
                    RelDir::Right => &rel.dst,
                    RelDir::Left => &rel.src,
                    RelDir::Both => {
                        if rel.src == src_node.id {
                            &rel.dst
                        } else {
                            &rel.src
                        }
                    }
                };

                let dst_node = match graph.node_by_id(dst_id) {
                    Some(n) => n,
                    None => continue,
                };

                if !self.node_matches(next_np, &dst_node, binding)? {
                    continue;
                }

                let mut nb = binding.clone();
                if let Some(rv) = &rp.var {
                    nb.insert(rv.clone(), Value::Rel(rel));
                }
                if let Some(nv) = &next_np.var {
                    nb.insert(nv.clone(), Value::Node(dst_node));
                }
                result.push(nb);
            }

            Ok(result)
        }

        fn match_variable_hop(
            &self,
            rp: &RelPattern,
            next_np: &NodePattern,
            src_node: &NodeRef,
            binding: &Binding,
            graph: &dyn Graph,
        ) -> Result<Vec<Binding>> {
            let min = rp.min_hops.unwrap_or(1) as usize;
            let max = rp.max_hops.unwrap_or(min.max(10) as u32) as usize;
            let mut result = Vec::new();

            // BFS: (current_node, collected_rels, depth)
            let mut frontier: Vec<(NodeRef, Vec<RelRef>, usize)> =
                vec![(src_node.clone(), vec![], 0)];

            while let Some((node, rels, depth)) = frontier.pop() {
                if depth >= min && depth <= max {
                    if self.node_matches(next_np, &node, binding)? {
                        let mut nb = binding.clone();
                        if let Some(rv) = &rp.var {
                            nb.insert(
                                rv.clone(),
                                Value::List(
                                    rels.iter().map(|r| Value::Rel(r.clone())).collect(),
                                ),
                            );
                        }
                        if let Some(nv) = &next_np.var {
                            nb.insert(nv.clone(), Value::Node(node.clone()));
                        }
                        result.push(nb);
                    }
                }
                if depth < max {
                    let candidates: Vec<RelRef> = match rp.dir {
                        RelDir::Right | RelDir::Both => {
                            let mut v = graph.rels_from(&node.id);
                            if matches!(rp.dir, RelDir::Both) {
                                v.extend(graph.rels_to(&node.id));
                            }
                            v
                        }
                        RelDir::Left => graph.rels_to(&node.id),
                    };
                    for rel in candidates {
                        if !rp.types.is_empty() && !rp.types.contains(&rel.rel_type) {
                            continue;
                        }
                        // Avoid cycles within same path
                        if rels.iter().any(|r| r.id == rel.id) {
                            continue;
                        }
                        let next_id = match rp.dir {
                            RelDir::Right => &rel.dst,
                            RelDir::Left => &rel.src,
                            RelDir::Both => {
                                if rel.src == node.id {
                                    &rel.dst
                                } else {
                                    &rel.src
                                }
                            }
                        };
                        if let Some(next_node) = graph.node_by_id(next_id) {
                            let mut new_rels = rels.clone();
                            new_rels.push(rel);
                            frontier.push((next_node, new_rels, depth + 1));
                        }
                    }
                }
            }
            Ok(result)
        }

        // ---- CREATE ---------------------------------------------------------

        fn execute_create(
            &self,
            patterns: &[Pattern],
            bindings: &mut Vec<Binding>,
            graph: &mut dyn Graph,
        ) -> Result<()> {
            for binding in bindings.iter_mut() {
                for pattern in patterns {
                    let var_ids = self.create_pattern(pattern, binding, graph)?;
                    for (var, id) in &var_ids {
                        if let Some(n) = graph.node_by_id(id) {
                            binding.insert(var.clone(), Value::Node(n.clone()));
                        } else if let Some(r) = graph.rel_by_id(id) {
                            binding.insert(var.clone(), Value::Rel(r.clone()));
                        }
                    }
                }
            }
            Ok(())
        }

        fn create_pattern(
            &self,
            pattern: &Pattern,
            binding: &Binding,
            graph: &mut dyn Graph,
        ) -> Result<IndexMap<String, String>> {
            // Returns var -> id mapping for created elements
            let mut var_ids: IndexMap<String, String> = IndexMap::new();

            let elements = &pattern.elements;
            let mut prev_node_id: Option<String> = None;

            let mut i = 0;
            while i < elements.len() {
                match &elements[i] {
                    PatternElement::Node(np) => {
                        // Check if variable already bound (from earlier pattern in same CREATE)
                        let existing_id = np.var.as_ref().and_then(|v| {
                            var_ids.get(v).cloned().or_else(|| {
                                binding.get(v).and_then(|val| match val {
                                    Value::Node(n) => Some(n.id.clone()),
                                    _ => None,
                                })
                            })
                        });

                        let id = if let Some(eid) = existing_id {
                            // Reuse existing node — don't create a duplicate
                            eid
                        } else {
                            let id = uuid::Uuid::new_v4().to_string();
                            let mut props = IndexMap::new();
                            for (k, expr) in &np.props {
                                props.insert(k.clone(), self.eval(expr, binding)?);
                            }
                            let node = NodeRef {
                                id: id.clone(),
                                labels: np.labels.clone(),
                                props,
                            };
                            graph.add_node(node);
                            if let Some(var) = &np.var {
                                var_ids.insert(var.clone(), id.clone());
                            }
                            id
                        };
                        prev_node_id = Some(id);
                        i += 1;
                    }
                    PatternElement::Rel(rp) => {
                        let next_np = match elements.get(i + 1) {
                            Some(PatternElement::Node(np)) => np,
                            _ => {
                                return Err(CypherError::ParseError(
                                    "dangling rel in CREATE pattern".into(),
                                ))
                            }
                        };

                        // Check if dst variable already bound
                        let existing_dst = next_np.var.as_ref().and_then(|v| {
                            var_ids.get(v).cloned().or_else(|| {
                                binding.get(v).and_then(|val| match val {
                                    Value::Node(n) => Some(n.id.clone()),
                                    _ => None,
                                })
                            })
                        });

                        let dst_id = if let Some(eid) = existing_dst {
                            eid
                        } else {
                            let dst_id = uuid::Uuid::new_v4().to_string();
                            let mut dst_props = IndexMap::new();
                            for (k, expr) in &next_np.props {
                                dst_props.insert(k.clone(), self.eval(expr, binding)?);
                            }
                            let dst_node = NodeRef {
                                id: dst_id.clone(),
                                labels: next_np.labels.clone(),
                                props: dst_props,
                            };
                            graph.add_node(dst_node);
                            if let Some(var) = &next_np.var {
                                var_ids.insert(var.clone(), dst_id.clone());
                            }
                            dst_id
                        };

                        let rel_id = uuid::Uuid::new_v4().to_string();
                        let mut rel_props = IndexMap::new();
                        for (k, expr) in &rp.props {
                            rel_props.insert(k.clone(), self.eval(expr, binding)?);
                        }

                        let (src, dst) = match rp.dir {
                            RelDir::Right | RelDir::Both => (
                                prev_node_id.clone().unwrap_or_default(),
                                dst_id.clone(),
                            ),
                            RelDir::Left => (
                                dst_id.clone(),
                                prev_node_id.clone().unwrap_or_default(),
                            ),
                        };

                        let rel_type = rp.types.first().cloned().unwrap_or_default();
                        let rel = RelRef {
                            id: rel_id.clone(),
                            rel_type,
                            src,
                            dst,
                            props: rel_props,
                        };
                        graph.add_rel(rel);

                        if let Some(rv) = &rp.var {
                            var_ids.insert(rv.clone(), rel_id);
                        }
                        prev_node_id = Some(dst_id);
                        i += 2; // skip rel + dst node (we handled dst node here)
                    }
                }
            }

            Ok(var_ids)
        }

        // ---- MERGE ----------------------------------------------------------

        fn execute_merge(
            &self,
            pattern: &Pattern,
            on_create: &[SetItem],
            on_match: &[SetItem],
            bindings: &mut Vec<Binding>,
            graph: &mut dyn Graph,
        ) -> Result<()> {
            let binding = bindings.first().cloned().unwrap_or_default();
            let matched = self.match_pattern(pattern, &binding, graph)?;
            if matched.is_empty() {
                let var_ids = self.create_pattern(pattern, &binding, graph)?;
                let mut new_binding = binding.clone();
                for (var, id) in &var_ids {
                    if let Some(n) = graph.node_by_id(id) {
                        new_binding.insert(var.clone(), Value::Node(n.clone()));
                    } else if let Some(r) = graph.rel_by_id(id) {
                        new_binding.insert(var.clone(), Value::Rel(r.clone()));
                    }
                }
                *bindings = vec![new_binding];
                if !on_create.is_empty() {
                    self.execute_set(on_create, bindings, graph)?;
                }
            } else {
                *bindings = matched;
                if !on_match.is_empty() {
                    self.execute_set(on_match, bindings, graph)?;
                }
            }
            Ok(())
        }

        // ---- SET ------------------------------------------------------------

        fn execute_set(
            &self,
            items: &[SetItem],
            bindings: &[Binding],
            graph: &mut dyn Graph,
        ) -> Result<()> {
            for binding in bindings {
                for item in items {
                    match item {
                        SetItem::PropSet(lhs, rhs) => {
                            let val = self.eval(rhs, binding)?;
                            match lhs {
                                Expr::Prop(base, key) => {
                                    let base_val = self.eval(base, binding)?;
                                    match base_val {
                                        Value::Node(n) => graph.set_node_prop(&n.id, key, val),
                                        Value::Rel(_r) => {
                                            // Rel prop setting — requires graph extension
                                        }
                                        _ => {}
                                    }
                                }
                                Expr::Var(var) => {
                                    // SET n = {map} or SET n += {map}
                                    if let Some(Value::Node(n)) = binding.get(var) {
                                        if let Value::Map(m) = val {
                                            for (k, v) in m {
                                                graph.set_node_prop(&n.id, &k, v);
                                            }
                                        }
                                    }
                                }
                                _ => {}
                            }
                        }
                        SetItem::LabelSet(var, labels) => {
                            if let Some(Value::Node(n)) = binding.get(var) {
                                graph.set_node_labels(&n.id, labels);
                            }
                        }
                    }
                }
            }
            Ok(())
        }

        // ---- DELETE ---------------------------------------------------------

        fn execute_delete(
            &self,
            exprs: &[Expr],
            detach: bool,
            bindings: &[Binding],
            graph: &mut dyn Graph,
        ) -> Result<()> {
            for binding in bindings {
                for expr in exprs {
                    let val = self.eval(expr, binding)?;
                    match val {
                        Value::Node(n) => {
                            if detach {
                                // Remove all attached rels
                                let to_remove: Vec<String> = graph
                                    .rels_from(&n.id)
                                    .iter()
                                    .chain(graph.rels_to(&n.id).iter())
                                    .map(|r| r.id.clone())
                                    .collect();
                                for rid in to_remove {
                                    graph.delete_rel(&rid);
                                }
                            }
                            graph.delete_node(&n.id);
                        }
                        Value::Rel(r) => {
                            graph.delete_rel(&r.id);
                        }
                        _ => {}
                    }
                }
            }
            Ok(())
        }

        // ---- FOREACH --------------------------------------------------------

        fn execute_foreach(
            &self,
            var: &str,
            list_expr: &Expr,
            body: &[Clause],
            bindings: &[Binding],
            graph: &mut dyn Graph,
        ) -> Result<()> {
            for binding in bindings {
                let list_val = self.eval(list_expr, binding)?;
                if let Value::List(items) = list_val {
                    for item in items {
                        let mut nb = binding.clone();
                        nb.insert(var.to_owned(), item);
                        let mut inner_bindings = vec![nb];
                        for clause in body {
                            match clause {
                                Clause::Create { patterns } => {
                                    self.execute_create(patterns, &mut inner_bindings, graph)?;
                                }
                                Clause::Set { items } => {
                                    self.execute_set(items, &inner_bindings, graph)?;
                                }
                                Clause::Delete { exprs, detach } => {
                                    self.execute_delete(exprs, *detach, &inner_bindings, graph)?;
                                }
                                Clause::Merge { pattern, on_create, on_match } => {
                                    self.execute_merge(pattern, on_create, on_match, &mut inner_bindings, graph)?;
                                }
                                Clause::Remove { items } => {
                                    self.execute_remove(items, &inner_bindings, graph)?;
                                }
                                _ => {}
                            }
                        }
                    }
                }
            }
            Ok(())
        }

        // ---- REMOVE ---------------------------------------------------------

        fn execute_remove(
            &self,
            items: &[SetItem],
            bindings: &[Binding],
            graph: &mut dyn Graph,
        ) -> Result<()> {
            for binding in bindings {
                for item in items {
                    match item {
                        SetItem::PropSet(lhs, _) => {
                            // REMOVE n.prop — set prop to null (effectively delete)
                            match lhs {
                                Expr::Prop(base, key) => {
                                    let base_val = self.eval(base, binding)?;
                                    if let Value::Node(n) = base_val {
                                        graph.set_node_prop(&n.id, key, Value::Null);
                                    }
                                }
                                _ => {}
                            }
                        }
                        SetItem::LabelSet(var, labels) => {
                            // REMOVE n:Label — not directly supported by Graph trait
                            // For now, noop
                            let _ = (var, labels);
                        }
                    }
                }
            }
            Ok(())
        }

        // ---- CALL {} subquery -------------------------------------------------

        fn execute_call(
            &self,
            subquery: &[Clause],
            bindings: &[Binding],
            graph: &mut dyn Graph,
        ) -> Result<Vec<Binding>> {
            let mut result = Vec::new();
            let clause_refs: Vec<&Clause> = subquery.iter().collect();
            for binding in bindings {
                // Execute subquery with current binding as starting context
                let mut inner_bindings: Vec<Binding> = vec![binding.clone()];
                let mut inner_result = ResultSet::empty();

                for clause in &clause_refs {
                    match clause {
                        Clause::Match { patterns, where_ } => {
                            inner_bindings = self.execute_match(patterns, where_, &inner_bindings, graph, false)?;
                        }
                        Clause::OptionalMatch { patterns, where_ } => {
                            inner_bindings = self.execute_match(patterns, where_, &inner_bindings, graph, true)?;
                        }
                        Clause::With { items, where_ } => {
                            inner_bindings = self.execute_with(items, where_, &inner_bindings)?;
                        }
                        Clause::Unwind { expr, alias } => {
                            inner_bindings = self.execute_unwind(expr, alias, &inner_bindings)?;
                        }
                        Clause::Return { items, distinct, order_by, limit, skip } => {
                            inner_result = self.execute_return(items, *distinct, order_by, limit, skip, &inner_bindings)?;
                            // Convert returned rows back to bindings
                            inner_bindings = inner_result.rows.iter().map(|r| {
                                let mut nb = binding.clone();
                                for (k, v) in &r.0 {
                                    nb.insert(k.clone(), v.clone());
                                }
                                nb
                            }).collect();
                        }
                        _ => {}
                    }
                }

                if inner_result.columns.is_empty() {
                    // No RETURN in subquery — just merge bindings
                    result.extend(inner_bindings);
                } else {
                    result.extend(inner_bindings);
                }
            }
            Ok(result)
        }

        // ---- CALL procedure(args) YIELD ... ------------------------------------

        fn execute_call_procedure(
            &self,
            name: &str,
            args: &[Expr],
            yields: &[String],
            bindings: &[Binding],
            graph: &dyn Graph,
        ) -> Result<Vec<Binding>> {
            match name.to_lowercase().as_str() {
                "db.index.vector.querynodes" => {
                    // args: label, embeddingProp, queryVector, k
                    if args.len() < 4 {
                        return Err(CypherError::TypeError(
                            "db.index.vector.queryNodes requires 4 arguments: label, embeddingProp, queryVector, k".into()
                        ));
                    }
                    let mut result = Vec::new();
                    for binding in bindings {
                        let label = match self.eval(&args[0], binding)? {
                            Value::Str(s) => s,
                            _ => return Err(CypherError::TypeError("queryNodes: label must be string".into())),
                        };
                        // args[1] is embedding property name (unused in MemoryGraph — always "embedding")
                        let query_vec = match self.eval(&args[2], binding)? {
                            Value::List(items) => {
                                let mut v = Vec::with_capacity(items.len());
                                for item in &items {
                                    match item {
                                        Value::Float(f) => v.push(*f as f32),
                                        Value::Int(i) => v.push(*i as f32),
                                        _ => return Err(CypherError::TypeError("queryNodes: vector elements must be numeric".into())),
                                    }
                                }
                                v
                            }
                            _ => return Err(CypherError::TypeError("queryNodes: queryVector must be a list".into())),
                        };
                        let k = match self.eval(&args[3], binding)? {
                            Value::Int(n) => n as usize,
                            _ => return Err(CypherError::TypeError("queryNodes: k must be integer".into())),
                        };
                        let labels = vec![label];
                        let hits = graph.vector_search(&query_vec, k, &labels);
                        for (node, score) in hits {
                            let mut nb = binding.clone();
                            // Bind YIELD columns: default "node" and "score"
                            let node_var = yields.first().map(|s| s.as_str()).unwrap_or("node");
                            let score_var = yields.get(1).map(|s| s.as_str()).unwrap_or("score");
                            nb.insert(node_var.to_owned(), Value::Node(node));
                            nb.insert(score_var.to_owned(), Value::Float(score));
                            result.push(nb);
                        }
                    }
                    Ok(result)
                }
                other => Err(CypherError::GraphError(format!("unknown procedure: {}", other))),
            }
        }

        // ---- UNWIND ---------------------------------------------------------

        fn execute_unwind(
            &self,
            expr: &Expr,
            alias: &str,
            bindings: &[Binding],
        ) -> Result<Vec<Binding>> {
            let mut result = Vec::new();
            for binding in bindings {
                let val = self.eval(expr, binding)?;
                match val {
                    Value::List(items) => {
                        for item in items {
                            let mut nb = binding.clone();
                            nb.insert(alias.to_owned(), item);
                            result.push(nb);
                        }
                    }
                    Value::Null => {
                        // UNWIND null produces no rows
                    }
                    _ => {
                        let mut nb = binding.clone();
                        nb.insert(alias.to_owned(), val);
                        result.push(nb);
                    }
                }
            }
            Ok(result)
        }

        // ---- WITH -----------------------------------------------------------

        fn execute_with(
            &self,
            items: &[ReturnItem],
            where_: &Option<Expr>,
            bindings: &[Binding],
        ) -> Result<Vec<Binding>> {
            let projected = self.project_bindings(items, bindings)?;
            if let Some(where_expr) = where_ {
                let mut filtered = Vec::new();
                for b in projected {
                    if self.eval(where_expr, &b)?.is_truthy() {
                        filtered.push(b);
                    }
                }
                Ok(filtered)
            } else {
                Ok(projected)
            }
        }

        fn project_bindings(
            &self,
            items: &[ReturnItem],
            bindings: &[Binding],
        ) -> Result<Vec<Binding>> {
            // Check for aggregation
            if self.has_aggregation(items) {
                return self.aggregate(items, bindings);
            }

            let mut result = Vec::new();
            for binding in bindings {
                let mut nb = IndexMap::new();
                for item in items {
                    if let Expr::Var(v) = &item.expr {
                        if v == "*" {
                            for (k, val) in binding {
                                nb.insert(k.clone(), val.clone());
                            }
                            continue;
                        }
                    }
                    let val = self.eval(&item.expr, binding)?;
                    let key = item
                        .alias
                        .clone()
                        .unwrap_or_else(|| self.expr_name(&item.expr));
                    nb.insert(key, val);
                }
                result.push(nb);
            }
            Ok(result)
        }

        fn expr_name(&self, expr: &Expr) -> String {
            match expr {
                Expr::Var(v) => v.clone(),
                Expr::Prop(base, key) => format!("{}.{}", self.expr_name(base), key),
                Expr::FnCall(name, args) => {
                    if args.is_empty() {
                        format!("{}()", name)
                    } else {
                        format!("{}({})", name, self.expr_name(&args[0]))
                    }
                }
                _ => "value".into(),
            }
        }

        // ---- RETURN ---------------------------------------------------------

        fn execute_return(
            &self,
            items: &[ReturnItem],
            distinct: bool,
            order_by: &[OrderItem],
            limit: &Option<Expr>,
            skip: &Option<Expr>,
            bindings: &[Binding],
        ) -> Result<ResultSet> {
            let mut projected = self.project_bindings(items, bindings)?;

            // Deduplicate if DISTINCT
            if distinct {
                projected = self.dedup_bindings(projected);
            }

            // ORDER BY
            if !order_by.is_empty() {
                let order_by = order_by.to_vec();
                let mut errors: Vec<CypherError> = Vec::new();
                projected.sort_by(|a, b| {
                    for item in &order_by {
                        let va = self.eval(&item.expr, a).unwrap_or(Value::Null);
                        let vb = self.eval(&item.expr, b).unwrap_or(Value::Null);
                        let ord = va.partial_cmp_val(&vb);
                        if let Some(o) = ord {
                            let o = if item.asc { o } else { o.reverse() };
                            if o != std::cmp::Ordering::Equal {
                                return o;
                            }
                        }
                    }
                    std::cmp::Ordering::Equal
                });
                if !errors.is_empty() {
                    return Err(errors.remove(0));
                }
            }

            // SKIP
            if let Some(skip_expr) = skip {
                let skip_n = match self.eval(skip_expr, &IndexMap::new())? {
                    Value::Int(n) => n as usize,
                    _ => 0,
                };
                if skip_n < projected.len() {
                    projected = projected[skip_n..].to_vec();
                } else {
                    projected = Vec::new();
                }
            }

            // LIMIT
            if let Some(limit_expr) = limit {
                let limit_n = match self.eval(limit_expr, &IndexMap::new())? {
                    Value::Int(n) => n as usize,
                    _ => usize::MAX,
                };
                projected.truncate(limit_n);
            }

            // Build ResultSet
            let columns: Vec<String> = if projected.is_empty() {
                self.infer_columns(items)
            } else {
                projected[0].keys().cloned().collect()
            };

            let rows = projected.into_iter().map(|b| Row(b)).collect();

            Ok(ResultSet { columns, rows })
        }

        fn infer_columns(&self, items: &[ReturnItem]) -> Vec<String> {
            items
                .iter()
                .map(|item| {
                    item.alias
                        .clone()
                        .unwrap_or_else(|| self.expr_name(&item.expr))
                })
                .collect()
        }

        fn dedup_bindings(&self, bindings: Vec<Binding>) -> Vec<Binding> {
            let mut seen: Vec<Vec<(String, String)>> = Vec::new();
            let mut result = Vec::new();
            for b in bindings {
                let key: Vec<(String, String)> =
                    b.iter().map(|(k, v)| (k.clone(), v.to_string())).collect();
                if !seen.contains(&key) {
                    seen.push(key);
                    result.push(b);
                }
            }
            result
        }

        // ---- Aggregation ----------------------------------------------------

        fn has_aggregation(&self, items: &[ReturnItem]) -> bool {
            items.iter().any(|item| self.expr_has_agg(&item.expr))
        }

        fn expr_has_agg(&self, expr: &Expr) -> bool {
            match expr {
                Expr::FnCall(name, _) => matches!(
                    name.as_str(),
                    "count" | "count_distinct" | "collect" | "sum" | "avg" | "min" | "max"
                ),
                Expr::BinOp(_, a, b) => self.expr_has_agg(a) || self.expr_has_agg(b),
                Expr::UnOp(_, a) => self.expr_has_agg(a),
                _ => false,
            }
        }

        fn aggregate(
            &self,
            items: &[ReturnItem],
            bindings: &[Binding],
        ) -> Result<Vec<Binding>> {
            // Separate grouping keys from aggregation expressions
            let (group_items, agg_items): (Vec<_>, Vec<_>) = items
                .iter()
                .partition(|item| !self.expr_has_agg(&item.expr));

            // Group bindings by key
            let mut groups: IndexMap<Vec<String>, Vec<&Binding>> = IndexMap::new();
            for b in bindings {
                let mut key_parts = Vec::new();
                for item in &group_items {
                    let v = self.eval(&item.expr, b).unwrap_or(Value::Null);
                    key_parts.push(v.to_string());
                }
                groups.entry(key_parts).or_default().push(b);
            }

            // If no group keys and no bindings, still produce one row for aggregation
            if groups.is_empty() && !bindings.is_empty() {
                let empty_key: Vec<String> = Vec::new();
                groups.insert(empty_key, bindings.iter().collect());
            } else if groups.is_empty() {
                // No input rows — count(*) should return 0
                let empty_key: Vec<String> = Vec::new();
                groups.insert(empty_key, Vec::new());
            }

            let mut result = Vec::new();
            for (_, group_bindings) in groups {
                let representative = group_bindings.first().copied();
                let mut nb: Binding = IndexMap::new();

                // Add group key columns
                for item in &group_items {
                    let val = representative
                        .map(|b| self.eval(&item.expr, b).unwrap_or(Value::Null))
                        .unwrap_or(Value::Null);
                    let key = item
                        .alias
                        .clone()
                        .unwrap_or_else(|| self.expr_name(&item.expr));
                    nb.insert(key, val);
                }

                // Compute aggregates
                for item in &agg_items {
                    let val = self.compute_agg(&item.expr, &group_bindings)?;
                    let key = item
                        .alias
                        .clone()
                        .unwrap_or_else(|| self.expr_name(&item.expr));
                    nb.insert(key, val);
                }

                result.push(nb);
            }

            Ok(result)
        }

        fn compute_agg(&self, expr: &Expr, group: &[&Binding]) -> Result<Value> {
            match expr {
                Expr::FnCall(name, args) => match name.as_str() {
                    "count" => {
                        if let Some(Expr::Var(v)) = args.first() {
                            if v == "*" {
                                return Ok(Value::Int(group.len() as i64));
                            }
                        }
                        // count(expr) — count non-null
                        let count = group
                            .iter()
                            .filter(|b| {
                                args.first()
                                    .and_then(|e| self.eval(e, b).ok())
                                    .map(|v| !matches!(v, Value::Null))
                                    .unwrap_or(false)
                            })
                            .count();
                        Ok(Value::Int(count as i64))
                    }
                    "count_distinct" => {
                        let arg = args
                            .first()
                            .ok_or_else(|| CypherError::TypeError("count(DISTINCT) requires arg".into()))?;
                        let mut seen: Vec<String> = Vec::new();
                        for b in group {
                            let v = self.eval(arg, b)?;
                            if matches!(v, Value::Null) {
                                continue;
                            }
                            let key = v.to_string();
                            if !seen.contains(&key) {
                                seen.push(key);
                            }
                        }
                        Ok(Value::Int(seen.len() as i64))
                    }
                    "collect" => {
                        let arg = args
                            .first()
                            .ok_or_else(|| CypherError::TypeError("collect requires arg".into()))?;
                        let items: Vec<Value> = group
                            .iter()
                            .filter_map(|b| {
                                let v = self.eval(arg, b).ok()?;
                                if matches!(v, Value::Null) {
                                    None
                                } else {
                                    Some(v)
                                }
                            })
                            .collect();
                        Ok(Value::List(items))
                    }
                    "sum" => {
                        let arg = args
                            .first()
                            .ok_or_else(|| CypherError::TypeError("sum requires arg".into()))?;
                        let mut int_sum: i64 = 0;
                        let mut float_sum: f64 = 0.0;
                        let mut is_float = false;
                        for b in group {
                            match self.eval(arg, b)? {
                                Value::Int(n) => int_sum += n,
                                Value::Float(f) => {
                                    is_float = true;
                                    float_sum += f;
                                }
                                Value::Null => {}
                                _ => {}
                            }
                        }
                        if is_float {
                            Ok(Value::Float(float_sum + int_sum as f64))
                        } else {
                            Ok(Value::Int(int_sum))
                        }
                    }
                    "avg" => {
                        let arg = args
                            .first()
                            .ok_or_else(|| CypherError::TypeError("avg requires arg".into()))?;
                        let mut sum: f64 = 0.0;
                        let mut count = 0usize;
                        for b in group {
                            match self.eval(arg, b)? {
                                Value::Int(n) => {
                                    sum += n as f64;
                                    count += 1;
                                }
                                Value::Float(f) => {
                                    sum += f;
                                    count += 1;
                                }
                                Value::Null => {}
                                _ => {}
                            }
                        }
                        if count == 0 {
                            Ok(Value::Null)
                        } else {
                            Ok(Value::Float(sum / count as f64))
                        }
                    }
                    "min" => {
                        let arg = args
                            .first()
                            .ok_or_else(|| CypherError::TypeError("min requires arg".into()))?;
                        let mut min_val: Option<Value> = None;
                        for b in group {
                            let v = self.eval(arg, b)?;
                            if matches!(v, Value::Null) {
                                continue;
                            }
                            min_val = Some(match min_val {
                                None => v,
                                Some(curr) => {
                                    if v.partial_cmp_val(&curr)
                                        == Some(std::cmp::Ordering::Less)
                                    {
                                        v
                                    } else {
                                        curr
                                    }
                                }
                            });
                        }
                        Ok(min_val.unwrap_or(Value::Null))
                    }
                    "max" => {
                        let arg = args
                            .first()
                            .ok_or_else(|| CypherError::TypeError("max requires arg".into()))?;
                        let mut max_val: Option<Value> = None;
                        for b in group {
                            let v = self.eval(arg, b)?;
                            if matches!(v, Value::Null) {
                                continue;
                            }
                            max_val = Some(match max_val {
                                None => v,
                                Some(curr) => {
                                    if v.partial_cmp_val(&curr)
                                        == Some(std::cmp::Ordering::Greater)
                                    {
                                        v
                                    } else {
                                        curr
                                    }
                                }
                            });
                        }
                        Ok(max_val.unwrap_or(Value::Null))
                    }
                    _ => {
                        // Non-aggregate function in agg context
                        let b = group.first().copied().cloned().unwrap_or_default();
                        self.eval(expr, &b)
                    }
                },
                _ => {
                    let b = group.first().copied().cloned().unwrap_or_default();
                    self.eval(expr, &b)
                }
            }
        }

        // ---- Expression evaluator -------------------------------------------

        pub fn eval(&self, expr: &Expr, binding: &Binding) -> Result<Value> {
            match expr {
                Expr::Lit(lit) => Ok(match lit {
                    Literal::Null => Value::Null,
                    Literal::Bool(b) => Value::Bool(*b),
                    Literal::Int(n) => Value::Int(*n),
                    Literal::Float(f) => Value::Float(*f),
                    Literal::Str(s) => Value::Str(s.clone()),
                }),

                Expr::Var(v) => {
                    if v == "*" {
                        return Ok(Value::Null);
                    }
                    binding
                        .get(v)
                        .cloned()
                        .ok_or_else(|| CypherError::UnboundVariable(v.clone()))
                }

                Expr::Param(name) => self
                    .params
                    .get(name)
                    .cloned()
                    .ok_or_else(|| CypherError::UnboundVariable(format!("${}", name))),

                Expr::Prop(base, key) => {
                    let base_val = self.eval(base, binding)?;
                    match base_val {
                        Value::Node(n) => Ok(n.props.get(key).cloned().unwrap_or(Value::Null)),
                        Value::Rel(r) => Ok(r.props.get(key).cloned().unwrap_or(Value::Null)),
                        Value::Map(m) => Ok(m.get(key).cloned().unwrap_or(Value::Null)),
                        Value::Null => Ok(Value::Null),
                        other => Err(CypherError::TypeError(format!(
                            "property access on {} (not a node/rel/map)",
                            other.type_name()
                        ))),
                    }
                }

                Expr::BinOp(op, lhs, rhs) => self.eval_binop(op, lhs, rhs, binding),

                Expr::UnOp(op, inner) => {
                    let v = self.eval(inner, binding)?;
                    match op {
                        UnOp::Neg => match v {
                            Value::Int(n) => Ok(Value::Int(-n)),
                            Value::Float(f) => Ok(Value::Float(-f)),
                            other => Err(CypherError::TypeError(format!(
                                "cannot negate {}",
                                other.type_name()
                            ))),
                        },
                        UnOp::Not => Ok(Value::Bool(!v.is_truthy())),
                    }
                }

                Expr::IsNull(inner) => {
                    let v = self.eval(inner, binding)?;
                    Ok(Value::Bool(matches!(v, Value::Null)))
                }

                Expr::IsNotNull(inner) => {
                    let v = self.eval(inner, binding)?;
                    Ok(Value::Bool(!matches!(v, Value::Null)))
                }

                Expr::In(elem, list) => {
                    let elem_val = self.eval(elem, binding)?;
                    let list_val = self.eval(list, binding)?;
                    match list_val {
                        Value::List(items) => {
                            Ok(Value::Bool(items.iter().any(|i| i.eq_val(&elem_val))))
                        }
                        Value::Null => Ok(Value::Null),
                        other => Err(CypherError::TypeError(format!(
                            "IN requires list, got {}",
                            other.type_name()
                        ))),
                    }
                }

                Expr::FnCall(name, args) => self.eval_fn(name, args, binding),

                Expr::List(items) => {
                    let mut vals = Vec::new();
                    for item in items {
                        vals.push(self.eval(item, binding)?);
                    }
                    Ok(Value::List(vals))
                }

                Expr::Map(entries) => {
                    let mut m = IndexMap::new();
                    for (k, v) in entries {
                        m.insert(k.clone(), self.eval(v, binding)?);
                    }
                    Ok(Value::Map(m))
                }

                Expr::Case {
                    scrutinee,
                    whens,
                    else_,
                } => {
                    let scrut = scrutinee
                        .as_ref()
                        .map(|e| self.eval(e, binding))
                        .transpose()?;
                    for (cond, result) in whens {
                        let matches = if let Some(s) = &scrut {
                            let c = self.eval(cond, binding)?;
                            s.eq_val(&c)
                        } else {
                            self.eval(cond, binding)?.is_truthy()
                        };
                        if matches {
                            return self.eval(result, binding);
                        }
                    }
                    if let Some(else_expr) = else_ {
                        self.eval(else_expr, binding)
                    } else {
                        Ok(Value::Null)
                    }
                }

                Expr::StartsWith(lhs, rhs) => {
                    let lv = self.eval(lhs, binding)?;
                    let rv = self.eval(rhs, binding)?;
                    match (lv, rv) {
                        (Value::Str(a), Value::Str(b)) => Ok(Value::Bool(a.starts_with(&*b))),
                        (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
                        _ => Ok(Value::Bool(false)),
                    }
                }

                Expr::EndsWith(lhs, rhs) => {
                    let lv = self.eval(lhs, binding)?;
                    let rv = self.eval(rhs, binding)?;
                    match (lv, rv) {
                        (Value::Str(a), Value::Str(b)) => Ok(Value::Bool(a.ends_with(&*b))),
                        (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
                        _ => Ok(Value::Bool(false)),
                    }
                }

                Expr::Contains(lhs, rhs) => {
                    let lv = self.eval(lhs, binding)?;
                    let rv = self.eval(rhs, binding)?;
                    match (lv, rv) {
                        (Value::Str(a), Value::Str(b)) => Ok(Value::Bool(a.contains(&*b))),
                        (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
                        _ => Ok(Value::Bool(false)),
                    }
                }

                Expr::RegexMatch(lhs, rhs) => {
                    let lv = self.eval(lhs, binding)?;
                    let rv = self.eval(rhs, binding)?;
                    match (lv, rv) {
                        (Value::Str(a), Value::Str(pattern)) => {
                            let re = regex::Regex::new(&pattern).map_err(|e| {
                                CypherError::TypeError(format!("invalid regex: {}", e))
                            })?;
                            Ok(Value::Bool(re.is_match(&a)))
                        }
                        (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
                        _ => Ok(Value::Bool(false)),
                    }
                }

                Expr::Index(base, idx) => {
                    let base_val = self.eval(base, binding)?;
                    let idx_val = self.eval(idx, binding)?;
                    match (base_val, idx_val) {
                        (Value::List(l), Value::Int(i)) => {
                            let index = if i < 0 {
                                l.len().checked_sub((-i) as usize)
                            } else {
                                Some(i as usize)
                            };
                            match index {
                                Some(idx) if idx < l.len() => Ok(l[idx].clone()),
                                _ => Ok(Value::Null),
                            }
                        }
                        (Value::Map(m), Value::Str(k)) => {
                            Ok(m.get(&k).cloned().unwrap_or(Value::Null))
                        }
                        (Value::Null, _) => Ok(Value::Null),
                        _ => Ok(Value::Null),
                    }
                }

                Expr::ListComp { var, list_expr, filter, map_expr } => {
                    let list_val = self.eval(list_expr, binding)?;
                    match list_val {
                        Value::List(items) => {
                            let mut result = Vec::new();
                            for item in items {
                                let mut nb = binding.clone();
                                nb.insert(var.clone(), item);
                                let include = if let Some(f) = filter {
                                    self.eval(f, &nb)?.is_truthy()
                                } else {
                                    true
                                };
                                if include {
                                    let val = if let Some(me) = map_expr {
                                        self.eval(me, &nb)?
                                    } else {
                                        nb.get(var).cloned().unwrap_or(Value::Null)
                                    };
                                    result.push(val);
                                }
                            }
                            Ok(Value::List(result))
                        }
                        Value::Null => Ok(Value::Null),
                        _ => Err(CypherError::TypeError("list comprehension requires list".into())),
                    }
                }

                Expr::MapProjection { base, entries } => {
                    let base_val = self.eval(base, binding)?;
                    let base_props = match &base_val {
                        Value::Node(n) => &n.props,
                        Value::Map(m) => m,
                        Value::Rel(r) => &r.props,
                        _ => return Err(CypherError::TypeError(
                            "map projection requires node, rel, or map".into(),
                        )),
                    };
                    let mut result = IndexMap::new();
                    for entry in entries {
                        match entry {
                            MapProjEntry::Prop(name) => {
                                let val = base_props.get(name).cloned().unwrap_or(Value::Null);
                                result.insert(name.clone(), val);
                            }
                            MapProjEntry::Literal(key, expr) => {
                                let val = self.eval(expr, binding)?;
                                result.insert(key.clone(), val);
                            }
                            MapProjEntry::AllProps => {
                                for (k, v) in base_props {
                                    result.insert(k.clone(), v.clone());
                                }
                            }
                        }
                    }
                    Ok(Value::Map(result))
                }

                Expr::ExistsSubquery(_) => {
                    // EXISTS { subquery } requires graph access — use eval_with_graph instead
                    Err(CypherError::TypeError(
                        "EXISTS { subquery } requires graph context; use eval_with_graph".into(),
                    ))
                }

                Expr::ListPredicate { kind, var, list_expr, pred } => {
                    let list_val = self.eval(list_expr, binding)?;
                    match list_val {
                        Value::List(items) => {
                            let mut count = 0usize;
                            let total = items.len();
                            for item in &items {
                                let mut nb = binding.clone();
                                nb.insert(var.clone(), item.clone());
                                if self.eval(pred, &nb)?.is_truthy() {
                                    count += 1;
                                }
                            }
                            let result = match kind {
                                ListPredicateKind::All => count == total,
                                ListPredicateKind::Any => count > 0,
                                ListPredicateKind::None => count == 0,
                                ListPredicateKind::Single => count == 1,
                            };
                            Ok(Value::Bool(result))
                        }
                        Value::Null => Ok(Value::Null),
                        _ => Err(CypherError::TypeError("list predicate requires list".into())),
                    }
                }
            }
        }

        /// Evaluate expression with graph access (needed for EXISTS subquery)
        pub fn eval_with_graph(&self, expr: &Expr, binding: &Binding, graph: &dyn Graph) -> Result<Value> {
            match expr {
                Expr::ExistsSubquery(clauses) => {
                    let mut current: Vec<Binding> = vec![binding.clone()];
                    for clause in clauses {
                        match clause {
                            Clause::Match { patterns, where_ } => {
                                current = self.execute_match(patterns, where_, &current, graph, false)?;
                            }
                            Clause::OptionalMatch { patterns, where_ } => {
                                current = self.execute_match(patterns, where_, &current, graph, true)?;
                            }
                            Clause::With { items, where_ } => {
                                current = self.execute_with(items, where_, &current)?;
                            }
                            _ => {}
                        }
                        if current.is_empty() {
                            return Ok(Value::Bool(false));
                        }
                    }
                    Ok(Value::Bool(!current.is_empty()))
                }
                // For all other expressions, delegate to eval but recursively handle sub-expressions
                // that may contain EXISTS
                Expr::BinOp(op, lhs, rhs) => {
                    // Check if sub-expressions contain EXISTS
                    if self.contains_exists(expr) {
                        let lv = self.eval_with_graph(lhs, binding, graph)?;
                        let rv = self.eval_with_graph(rhs, binding, graph)?;
                        // Re-evaluate with resolved values
                        let lhs_lit = self.value_to_expr(&lv);
                        let rhs_lit = self.value_to_expr(&rv);
                        self.eval_binop(op, &lhs_lit, &rhs_lit, binding)
                    } else {
                        self.eval(expr, binding)
                    }
                }
                Expr::UnOp(UnOp::Not, inner) => {
                    if self.contains_exists(inner) {
                        let v = self.eval_with_graph(inner, binding, graph)?;
                        Ok(Value::Bool(!v.is_truthy()))
                    } else {
                        self.eval(expr, binding)
                    }
                }
                _ => self.eval(expr, binding),
            }
        }

        fn contains_exists(&self, expr: &Expr) -> bool {
            match expr {
                Expr::ExistsSubquery(_) => true,
                Expr::BinOp(_, a, b) => self.contains_exists(a) || self.contains_exists(b),
                Expr::UnOp(_, a) => self.contains_exists(a),
                _ => false,
            }
        }

        fn value_to_expr(&self, v: &Value) -> Expr {
            match v {
                Value::Null => Expr::Lit(Literal::Null),
                Value::Bool(b) => Expr::Lit(Literal::Bool(*b)),
                Value::Int(n) => Expr::Lit(Literal::Int(*n)),
                Value::Float(f) => Expr::Lit(Literal::Float(*f)),
                Value::Str(s) => Expr::Lit(Literal::Str(s.clone())),
                _ => Expr::Lit(Literal::Null),
            }
        }

        fn eval_binop(
            &self,
            op: &BinOp,
            lhs: &Expr,
            rhs: &Expr,
            binding: &Binding,
        ) -> Result<Value> {
            // Short-circuit for AND/OR
            match op {
                BinOp::And => {
                    let lv = self.eval(lhs, binding)?;
                    if !lv.is_truthy() {
                        return Ok(Value::Bool(false));
                    }
                    let rv = self.eval(rhs, binding)?;
                    return Ok(Value::Bool(rv.is_truthy()));
                }
                BinOp::Or => {
                    let lv = self.eval(lhs, binding)?;
                    if lv.is_truthy() {
                        return Ok(Value::Bool(true));
                    }
                    let rv = self.eval(rhs, binding)?;
                    return Ok(Value::Bool(rv.is_truthy()));
                }
                _ => {}
            }

            let lv = self.eval(lhs, binding)?;
            let rv = self.eval(rhs, binding)?;

            match op {
                BinOp::Eq => Ok(Value::Bool(lv.eq_val(&rv))),
                BinOp::Neq => Ok(Value::Bool(!lv.eq_val(&rv))),
                BinOp::Lt => Ok(Value::Bool(
                    lv.partial_cmp_val(&rv) == Some(std::cmp::Ordering::Less),
                )),
                BinOp::Lte => Ok(Value::Bool(matches!(
                    lv.partial_cmp_val(&rv),
                    Some(std::cmp::Ordering::Less) | Some(std::cmp::Ordering::Equal)
                ))),
                BinOp::Gt => Ok(Value::Bool(
                    lv.partial_cmp_val(&rv) == Some(std::cmp::Ordering::Greater),
                )),
                BinOp::Gte => Ok(Value::Bool(matches!(
                    lv.partial_cmp_val(&rv),
                    Some(std::cmp::Ordering::Greater) | Some(std::cmp::Ordering::Equal)
                ))),
                BinOp::Add => match (lv, rv) {
                    (Value::Int(a), Value::Int(b)) => Ok(Value::Int(a + b)),
                    (Value::Float(a), Value::Float(b)) => Ok(Value::Float(a + b)),
                    (Value::Int(a), Value::Float(b)) => Ok(Value::Float(a as f64 + b)),
                    (Value::Float(a), Value::Int(b)) => Ok(Value::Float(a + b as f64)),
                    (Value::Str(a), Value::Str(b)) => Ok(Value::Str(a + &b)),
                    (Value::List(mut a), Value::List(b)) => {
                        a.extend(b);
                        Ok(Value::List(a))
                    }
                    (a, b) => Err(CypherError::TypeError(format!(
                        "cannot add {} and {}",
                        a.type_name(),
                        b.type_name()
                    ))),
                },
                BinOp::Sub => match (lv, rv) {
                    (Value::Int(a), Value::Int(b)) => Ok(Value::Int(a - b)),
                    (Value::Float(a), Value::Float(b)) => Ok(Value::Float(a - b)),
                    (Value::Int(a), Value::Float(b)) => Ok(Value::Float(a as f64 - b)),
                    (Value::Float(a), Value::Int(b)) => Ok(Value::Float(a - b as f64)),
                    (a, b) => Err(CypherError::TypeError(format!(
                        "cannot subtract {} from {}",
                        b.type_name(),
                        a.type_name()
                    ))),
                },
                BinOp::Mul => match (lv, rv) {
                    (Value::Int(a), Value::Int(b)) => Ok(Value::Int(a * b)),
                    (Value::Float(a), Value::Float(b)) => Ok(Value::Float(a * b)),
                    (Value::Int(a), Value::Float(b)) => Ok(Value::Float(a as f64 * b)),
                    (Value::Float(a), Value::Int(b)) => Ok(Value::Float(a * b as f64)),
                    (a, b) => Err(CypherError::TypeError(format!(
                        "cannot multiply {} and {}",
                        a.type_name(),
                        b.type_name()
                    ))),
                },
                BinOp::Div => match (lv, rv) {
                    (Value::Int(a), Value::Int(b)) => {
                        if b == 0 {
                            Err(CypherError::TypeError("division by zero".into()))
                        } else {
                            Ok(Value::Int(a / b))
                        }
                    }
                    (Value::Float(a), Value::Float(b)) => Ok(Value::Float(a / b)),
                    (Value::Int(a), Value::Float(b)) => Ok(Value::Float(a as f64 / b)),
                    (Value::Float(a), Value::Int(b)) => Ok(Value::Float(a / b as f64)),
                    (a, b) => Err(CypherError::TypeError(format!(
                        "cannot divide {} by {}",
                        a.type_name(),
                        b.type_name()
                    ))),
                },
                BinOp::Mod => match (lv, rv) {
                    (Value::Int(a), Value::Int(b)) => Ok(Value::Int(a % b)),
                    (Value::Float(a), Value::Float(b)) => Ok(Value::Float(a % b)),
                    (a, b) => Err(CypherError::TypeError(format!(
                        "cannot mod {} by {}",
                        a.type_name(),
                        b.type_name()
                    ))),
                },
                BinOp::Xor => Ok(Value::Bool(lv.is_truthy() ^ rv.is_truthy())),
                BinOp::Concat => match (lv, rv) {
                    (Value::Str(a), Value::Str(b)) => Ok(Value::Str(a + &b)),
                    (Value::List(mut a), Value::List(b)) => {
                        a.extend(b);
                        Ok(Value::List(a))
                    }
                    (a, b) => Err(CypherError::TypeError(format!(
                        "cannot concat {} and {}",
                        a.type_name(),
                        b.type_name()
                    ))),
                },
                BinOp::And | BinOp::Or => unreachable!("handled above"),
            }
        }

        fn eval_fn(&self, name: &str, args: &[Expr], binding: &Binding) -> Result<Value> {
            match name {
                "id" => {
                    let v = self.eval_arg(args, 0, binding)?;
                    match v {
                        Value::Node(n) => Ok(Value::Str(n.id)),
                        Value::Rel(r) => Ok(Value::Str(r.id)),
                        _ => Err(CypherError::TypeError("id() requires node or rel".into())),
                    }
                }
                "labels" => {
                    let v = self.eval_arg(args, 0, binding)?;
                    match v {
                        Value::Node(n) => Ok(Value::List(
                            n.labels.into_iter().map(Value::Str).collect(),
                        )),
                        _ => Err(CypherError::TypeError("labels() requires node".into())),
                    }
                }
                "type" => {
                    let v = self.eval_arg(args, 0, binding)?;
                    match v {
                        Value::Rel(r) => Ok(Value::Str(r.rel_type)),
                        _ => Err(CypherError::TypeError("type() requires relationship".into())),
                    }
                }
                "keys" => {
                    let v = self.eval_arg(args, 0, binding)?;
                    match v {
                        Value::Node(n) => Ok(Value::List(
                            n.props.keys().map(|k| Value::Str(k.clone())).collect(),
                        )),
                        Value::Rel(r) => Ok(Value::List(
                            r.props.keys().map(|k| Value::Str(k.clone())).collect(),
                        )),
                        Value::Map(m) => Ok(Value::List(
                            m.keys().map(|k| Value::Str(k.clone())).collect(),
                        )),
                        _ => Err(CypherError::TypeError(
                            "keys() requires node, rel, or map".into(),
                        )),
                    }
                }
                "properties" => {
                    let v = self.eval_arg(args, 0, binding)?;
                    match v {
                        Value::Node(n) => Ok(Value::Map(n.props)),
                        Value::Rel(r) => Ok(Value::Map(r.props)),
                        _ => Err(CypherError::TypeError(
                            "properties() requires node or rel".into(),
                        )),
                    }
                }
                "size" => {
                    let v = self.eval_arg(args, 0, binding)?;
                    match v {
                        Value::List(l) => Ok(Value::Int(l.len() as i64)),
                        Value::Str(s) => Ok(Value::Int(s.len() as i64)),
                        Value::Map(m) => Ok(Value::Int(m.len() as i64)),
                        _ => Err(CypherError::TypeError(
                            "size() requires list, string, or map".into(),
                        )),
                    }
                }
                "coalesce" => {
                    for arg in args {
                        let v = self.eval(arg, binding)?;
                        if !matches!(v, Value::Null) {
                            return Ok(v);
                        }
                    }
                    Ok(Value::Null)
                }
                "tointeger" | "toint" => {
                    let v = self.eval_arg(args, 0, binding)?;
                    match v {
                        Value::Int(n) => Ok(Value::Int(n)),
                        Value::Float(f) => Ok(Value::Int(f as i64)),
                        Value::Str(s) => s
                            .parse::<i64>()
                            .map(Value::Int)
                            .map_err(|_| CypherError::TypeError(format!("cannot parse '{}' as integer", s))),
                        Value::Null => Ok(Value::Null),
                        _ => Err(CypherError::TypeError("toInteger() requires numeric or string".into())),
                    }
                }
                "tostring" | "tostr" => {
                    let v = self.eval_arg(args, 0, binding)?;
                    Ok(Value::Str(v.to_string()))
                }
                "tolower" => {
                    let v = self.eval_arg(args, 0, binding)?;
                    match v {
                        Value::Str(s) => Ok(Value::Str(s.to_lowercase())),
                        Value::Null => Ok(Value::Null),
                        _ => Err(CypherError::TypeError("toLower() requires string".into())),
                    }
                }
                "toupper" => {
                    let v = self.eval_arg(args, 0, binding)?;
                    match v {
                        Value::Str(s) => Ok(Value::Str(s.to_uppercase())),
                        Value::Null => Ok(Value::Null),
                        _ => Err(CypherError::TypeError("toUpper() requires string".into())),
                    }
                }
                "exists" => {
                    // exists(n.prop) — check if prop is not null
                    let v = self.eval_arg(args, 0, binding)?;
                    Ok(Value::Bool(!matches!(v, Value::Null)))
                }
                "abs" => {
                    let v = self.eval_arg(args, 0, binding)?;
                    match v {
                        Value::Int(n) => Ok(Value::Int(n.abs())),
                        Value::Float(f) => Ok(Value::Float(f.abs())),
                        _ => Err(CypherError::TypeError("abs() requires numeric".into())),
                    }
                }
                "head" => {
                    let v = self.eval_arg(args, 0, binding)?;
                    match v {
                        Value::List(l) => Ok(l.into_iter().next().unwrap_or(Value::Null)),
                        _ => Err(CypherError::TypeError("head() requires list".into())),
                    }
                }
                "last" => {
                    let v = self.eval_arg(args, 0, binding)?;
                    match v {
                        Value::List(l) => Ok(l.into_iter().last().unwrap_or(Value::Null)),
                        _ => Err(CypherError::TypeError("last() requires list".into())),
                    }
                }
                "tail" => {
                    let v = self.eval_arg(args, 0, binding)?;
                    match v {
                        Value::List(l) => {
                            if l.is_empty() {
                                Ok(Value::List(vec![]))
                            } else {
                                Ok(Value::List(l[1..].to_vec()))
                            }
                        }
                        _ => Err(CypherError::TypeError("tail() requires list".into())),
                    }
                }
                "range" => {
                    let start = match self.eval_arg(args, 0, binding)? {
                        Value::Int(n) => n,
                        _ => return Err(CypherError::TypeError("range() requires integers".into())),
                    };
                    let end = match self.eval_arg(args, 1, binding)? {
                        Value::Int(n) => n,
                        _ => return Err(CypherError::TypeError("range() requires integers".into())),
                    };
                    let step = if args.len() > 2 {
                        match self.eval_arg(args, 2, binding)? {
                            Value::Int(n) => n,
                            _ => 1,
                        }
                    } else {
                        1
                    };
                    let mut result = Vec::new();
                    let mut i = start;
                    while if step > 0 { i <= end } else { i >= end } {
                        result.push(Value::Int(i));
                        i += step;
                    }
                    Ok(Value::List(result))
                }
                "substring" => {
                    let s = match self.eval_arg(args, 0, binding)? {
                        Value::Str(s) => s,
                        _ => return Err(CypherError::TypeError("substring() requires string".into())),
                    };
                    let start = match self.eval_arg(args, 1, binding)? {
                        Value::Int(n) => n as usize,
                        _ => 0,
                    };
                    let chars: Vec<char> = s.chars().collect();
                    let result = if args.len() > 2 {
                        let len = match self.eval_arg(args, 2, binding)? {
                            Value::Int(n) => n as usize,
                            _ => chars.len(),
                        };
                        chars[start.min(chars.len())..][..len.min(chars.len().saturating_sub(start))]
                            .iter()
                            .collect()
                    } else {
                        chars[start.min(chars.len())..].iter().collect()
                    };
                    Ok(Value::Str(result))
                }
                "trim" => {
                    let v = self.eval_arg(args, 0, binding)?;
                    match v {
                        Value::Str(s) => Ok(Value::Str(s.trim().to_owned())),
                        Value::Null => Ok(Value::Null),
                        _ => Err(CypherError::TypeError("trim() requires string".into())),
                    }
                }
                // count/collect/sum/avg/min/max are handled in aggregate; if called outside agg context
                "count" => {
                    if let Some(Expr::Var(v)) = args.first() {
                        if v == "*" {
                            return Ok(Value::Int(1));
                        }
                    }
                    let v = self.eval_arg(args, 0, binding)?;
                    Ok(Value::Int(if matches!(v, Value::Null) { 0 } else { 1 }))
                }
                "tofloat" => {
                    let v = self.eval_arg(args, 0, binding)?;
                    match v {
                        Value::Float(f) => Ok(Value::Float(f)),
                        Value::Int(n) => Ok(Value::Float(n as f64)),
                        Value::Str(s) => s
                            .parse::<f64>()
                            .map(Value::Float)
                            .map_err(|_| CypherError::TypeError(format!("cannot parse '{}' as float", s))),
                        Value::Null => Ok(Value::Null),
                        _ => Err(CypherError::TypeError("toFloat() requires numeric or string".into())),
                    }
                }
                "ceil" => {
                    let v = self.eval_arg(args, 0, binding)?;
                    match v {
                        Value::Float(f) => Ok(Value::Float(f.ceil())),
                        Value::Int(n) => Ok(Value::Int(n)),
                        Value::Null => Ok(Value::Null),
                        _ => Err(CypherError::TypeError("ceil() requires numeric".into())),
                    }
                }
                "floor" => {
                    let v = self.eval_arg(args, 0, binding)?;
                    match v {
                        Value::Float(f) => Ok(Value::Float(f.floor())),
                        Value::Int(n) => Ok(Value::Int(n)),
                        Value::Null => Ok(Value::Null),
                        _ => Err(CypherError::TypeError("floor() requires numeric".into())),
                    }
                }
                "round" => {
                    let v = self.eval_arg(args, 0, binding)?;
                    match v {
                        Value::Float(f) => Ok(Value::Float(f.round())),
                        Value::Int(n) => Ok(Value::Int(n)),
                        Value::Null => Ok(Value::Null),
                        _ => Err(CypherError::TypeError("round() requires numeric".into())),
                    }
                }
                "sign" => {
                    let v = self.eval_arg(args, 0, binding)?;
                    match v {
                        Value::Int(n) => Ok(Value::Int(n.signum())),
                        Value::Float(f) => Ok(Value::Float(if f > 0.0 { 1.0 } else if f < 0.0 { -1.0 } else { 0.0 })),
                        Value::Null => Ok(Value::Null),
                        _ => Err(CypherError::TypeError("sign() requires numeric".into())),
                    }
                }
                "sqrt" => {
                    let v = self.eval_arg(args, 0, binding)?;
                    match v {
                        Value::Int(n) => Ok(Value::Float((n as f64).sqrt())),
                        Value::Float(f) => Ok(Value::Float(f.sqrt())),
                        Value::Null => Ok(Value::Null),
                        _ => Err(CypherError::TypeError("sqrt() requires numeric".into())),
                    }
                }
                "replace" => {
                    let s = match self.eval_arg(args, 0, binding)? {
                        Value::Str(s) => s,
                        Value::Null => return Ok(Value::Null),
                        _ => return Err(CypherError::TypeError("replace() requires string".into())),
                    };
                    let search = match self.eval_arg(args, 1, binding)? {
                        Value::Str(s) => s,
                        _ => return Err(CypherError::TypeError("replace() search must be string".into())),
                    };
                    let replacement = match self.eval_arg(args, 2, binding)? {
                        Value::Str(s) => s,
                        _ => return Err(CypherError::TypeError("replace() replacement must be string".into())),
                    };
                    Ok(Value::Str(s.replace(&*search, &replacement)))
                }
                "left" => {
                    let s = match self.eval_arg(args, 0, binding)? {
                        Value::Str(s) => s,
                        Value::Null => return Ok(Value::Null),
                        _ => return Err(CypherError::TypeError("left() requires string".into())),
                    };
                    let n = match self.eval_arg(args, 1, binding)? {
                        Value::Int(n) => n as usize,
                        _ => return Err(CypherError::TypeError("left() length must be integer".into())),
                    };
                    Ok(Value::Str(s.chars().take(n).collect()))
                }
                "right" => {
                    let s = match self.eval_arg(args, 0, binding)? {
                        Value::Str(s) => s,
                        Value::Null => return Ok(Value::Null),
                        _ => return Err(CypherError::TypeError("right() requires string".into())),
                    };
                    let n = match self.eval_arg(args, 1, binding)? {
                        Value::Int(n) => n as usize,
                        _ => return Err(CypherError::TypeError("right() length must be integer".into())),
                    };
                    let chars: Vec<char> = s.chars().collect();
                    let start = chars.len().saturating_sub(n);
                    Ok(Value::Str(chars[start..].iter().collect()))
                }
                "split" => {
                    let s = match self.eval_arg(args, 0, binding)? {
                        Value::Str(s) => s,
                        Value::Null => return Ok(Value::Null),
                        _ => return Err(CypherError::TypeError("split() requires string".into())),
                    };
                    let delim = match self.eval_arg(args, 1, binding)? {
                        Value::Str(s) => s,
                        _ => return Err(CypherError::TypeError("split() delimiter must be string".into())),
                    };
                    Ok(Value::List(
                        s.split(&*delim).map(|p| Value::Str(p.to_owned())).collect(),
                    ))
                }
                "reverse" => {
                    let v = self.eval_arg(args, 0, binding)?;
                    match v {
                        Value::Str(s) => Ok(Value::Str(s.chars().rev().collect())),
                        Value::List(mut l) => {
                            l.reverse();
                            Ok(Value::List(l))
                        }
                        Value::Null => Ok(Value::Null),
                        _ => Err(CypherError::TypeError("reverse() requires string or list".into())),
                    }
                }
                "length" => {
                    let v = self.eval_arg(args, 0, binding)?;
                    match v {
                        Value::List(l) => Ok(Value::Int(l.len() as i64)),
                        Value::Str(s) => Ok(Value::Int(s.chars().count() as i64)),
                        Value::Null => Ok(Value::Null),
                        _ => Err(CypherError::TypeError("length() requires list or string".into())),
                    }
                }
                // Trigonometric functions
                "sin" => {
                    let v = self.eval_arg(args, 0, binding)?;
                    self.numeric_fn(v, |f| f.sin())
                }
                "cos" => {
                    let v = self.eval_arg(args, 0, binding)?;
                    self.numeric_fn(v, |f| f.cos())
                }
                "tan" => {
                    let v = self.eval_arg(args, 0, binding)?;
                    self.numeric_fn(v, |f| f.tan())
                }
                "asin" => {
                    let v = self.eval_arg(args, 0, binding)?;
                    self.numeric_fn(v, |f| f.asin())
                }
                "acos" => {
                    let v = self.eval_arg(args, 0, binding)?;
                    self.numeric_fn(v, |f| f.acos())
                }
                "atan" => {
                    let v = self.eval_arg(args, 0, binding)?;
                    self.numeric_fn(v, |f| f.atan())
                }
                "atan2" => {
                    let y = self.eval_arg(args, 0, binding)?;
                    let x = self.eval_arg(args, 1, binding)?;
                    match (y, x) {
                        (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
                        (y, x) => {
                            let yf = match y { Value::Int(n) => n as f64, Value::Float(f) => f, _ => return Err(CypherError::TypeError("atan2() requires numeric".into())) };
                            let xf = match x { Value::Int(n) => n as f64, Value::Float(f) => f, _ => return Err(CypherError::TypeError("atan2() requires numeric".into())) };
                            Ok(Value::Float(yf.atan2(xf)))
                        }
                    }
                }
                "log" => {
                    let v = self.eval_arg(args, 0, binding)?;
                    self.numeric_fn(v, |f| f.ln())
                }
                "log10" => {
                    let v = self.eval_arg(args, 0, binding)?;
                    self.numeric_fn(v, |f| f.log10())
                }
                "exp" => {
                    let v = self.eval_arg(args, 0, binding)?;
                    self.numeric_fn(v, |f| f.exp())
                }
                "e" => Ok(Value::Float(std::f64::consts::E)),
                "pi" => Ok(Value::Float(std::f64::consts::PI)),
                "rand" => {
                    // Pseudo-random using time-based seed
                    let seed = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .map(|d| d.as_nanos())
                        .unwrap_or(0);
                    let r = ((seed ^ (seed >> 16)) & 0xFFFFFFFF) as f64 / 0xFFFFFFFF_u64 as f64;
                    Ok(Value::Float(r))
                }
                "degrees" => {
                    let v = self.eval_arg(args, 0, binding)?;
                    self.numeric_fn(v, |f| f.to_degrees())
                }
                "radians" => {
                    let v = self.eval_arg(args, 0, binding)?;
                    self.numeric_fn(v, |f| f.to_radians())
                }
                // Temporal functions
                "timestamp" => {
                    let ms = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .map(|d| d.as_millis() as i64)
                        .unwrap_or(0);
                    Ok(Value::Int(ms))
                }
                "date" => {
                    if args.is_empty() {
                        // Current date as ISO string
                        let ms = std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .map(|d| d.as_secs())
                            .unwrap_or(0);
                        let days = ms / 86400;
                        // Simple epoch-day to date (approx, good enough for basic use)
                        Ok(Value::Str(format!("epoch+{}d", days)))
                    } else {
                        let v = self.eval_arg(args, 0, binding)?;
                        match v {
                            Value::Str(s) => Ok(Value::Str(s)),
                            _ => Ok(Value::Null),
                        }
                    }
                }
                "datetime" | "localdatetime" => {
                    if args.is_empty() {
                        let ms = std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .map(|d| d.as_millis() as i64)
                            .unwrap_or(0);
                        Ok(Value::Int(ms))
                    } else {
                        let v = self.eval_arg(args, 0, binding)?;
                        match v {
                            Value::Str(s) => Ok(Value::Str(s)),
                            _ => Ok(Value::Null),
                        }
                    }
                }
                "duration" => {
                    if args.is_empty() {
                        Ok(Value::Null)
                    } else {
                        let v = self.eval_arg(args, 0, binding)?;
                        match v {
                            Value::Str(s) => Ok(Value::Str(s)),
                            _ => Ok(Value::Null),
                        }
                    }
                }
                // Additional string functions
                "ltrim" => {
                    let v = self.eval_arg(args, 0, binding)?;
                    match v {
                        Value::Str(s) => Ok(Value::Str(s.trim_start().to_owned())),
                        Value::Null => Ok(Value::Null),
                        _ => Err(CypherError::TypeError("lTrim() requires string".into())),
                    }
                }
                "rtrim" => {
                    let v = self.eval_arg(args, 0, binding)?;
                    match v {
                        Value::Str(s) => Ok(Value::Str(s.trim_end().to_owned())),
                        Value::Null => Ok(Value::Null),
                        _ => Err(CypherError::TypeError("rTrim() requires string".into())),
                    }
                }
                "lpad" => {
                    let s = match self.eval_arg(args, 0, binding)? {
                        Value::Str(s) => s,
                        Value::Null => return Ok(Value::Null),
                        _ => return Err(CypherError::TypeError("lpad() requires string".into())),
                    };
                    let width = match self.eval_arg(args, 1, binding)? {
                        Value::Int(n) => n as usize,
                        _ => return Err(CypherError::TypeError("lpad() width must be integer".into())),
                    };
                    let pad_char = if args.len() > 2 {
                        match self.eval_arg(args, 2, binding)? {
                            Value::Str(p) => p.chars().next().unwrap_or(' '),
                            _ => ' ',
                        }
                    } else {
                        ' '
                    };
                    if s.len() >= width {
                        Ok(Value::Str(s))
                    } else {
                        let padding: String = std::iter::repeat(pad_char).take(width - s.len()).collect();
                        Ok(Value::Str(format!("{}{}", padding, s)))
                    }
                }
                "rpad" => {
                    let s = match self.eval_arg(args, 0, binding)? {
                        Value::Str(s) => s,
                        Value::Null => return Ok(Value::Null),
                        _ => return Err(CypherError::TypeError("rpad() requires string".into())),
                    };
                    let width = match self.eval_arg(args, 1, binding)? {
                        Value::Int(n) => n as usize,
                        _ => return Err(CypherError::TypeError("rpad() width must be integer".into())),
                    };
                    let pad_char = if args.len() > 2 {
                        match self.eval_arg(args, 2, binding)? {
                            Value::Str(p) => p.chars().next().unwrap_or(' '),
                            _ => ' ',
                        }
                    } else {
                        ' '
                    };
                    if s.len() >= width {
                        Ok(Value::Str(s))
                    } else {
                        let padding: String = std::iter::repeat(pad_char).take(width - s.len()).collect();
                        Ok(Value::Str(format!("{}{}", s, padding)))
                    }
                }
                "toboolean" => {
                    let v = self.eval_arg(args, 0, binding)?;
                    match v {
                        Value::Bool(b) => Ok(Value::Bool(b)),
                        Value::Str(s) => match s.to_lowercase().as_str() {
                            "true" => Ok(Value::Bool(true)),
                            "false" => Ok(Value::Bool(false)),
                            _ => Ok(Value::Null),
                        },
                        Value::Int(n) => Ok(Value::Bool(n != 0)),
                        Value::Null => Ok(Value::Null),
                        _ => Ok(Value::Null),
                    }
                }
                // shortestPath requires graph — handled in shortestpath_fn; return null in plain eval context
                "shortestpath" | "allshortestpaths" => {
                    // These require graph access. Return null when called outside graph context.
                    Ok(Value::Null)
                }
                // Vector distance functions
                "cosine_distance" | "gds.similarity.cosine" => {
                    let a = self.eval_arg(args, 0, binding)?;
                    let b = self.eval_arg(args, 1, binding)?;
                    match (&a, &b) {
                        (Value::List(_), Value::List(_)) => {
                            if let (Some(va), Some(vb)) = (
                                crate::graph::extract_f32_vec(&a),
                                crate::graph::extract_f32_vec(&b),
                            ) {
                                Ok(Value::Float(1.0 - crate::graph::cosine_similarity(&va, &vb)))
                            } else {
                                Ok(Value::Null)
                            }
                        }
                        _ => Ok(Value::Null),
                    }
                }
                "cosine_similarity" => {
                    let a = self.eval_arg(args, 0, binding)?;
                    let b = self.eval_arg(args, 1, binding)?;
                    match (&a, &b) {
                        (Value::List(_), Value::List(_)) => {
                            if let (Some(va), Some(vb)) = (
                                crate::graph::extract_f32_vec(&a),
                                crate::graph::extract_f32_vec(&b),
                            ) {
                                Ok(Value::Float(crate::graph::cosine_similarity(&va, &vb)))
                            } else {
                                Ok(Value::Null)
                            }
                        }
                        _ => Ok(Value::Null),
                    }
                }
                "euclidean_distance" | "gds.similarity.euclidean" | "l2_distance" => {
                    let a = self.eval_arg(args, 0, binding)?;
                    let b = self.eval_arg(args, 1, binding)?;
                    match (&a, &b) {
                        (Value::List(_), Value::List(_)) => {
                            if let (Some(va), Some(vb)) = (
                                crate::graph::extract_f32_vec(&a),
                                crate::graph::extract_f32_vec(&b),
                            ) {
                                Ok(Value::Float(crate::graph::euclidean_distance(&va, &vb)))
                            } else {
                                Ok(Value::Null)
                            }
                        }
                        _ => Ok(Value::Null),
                    }
                }
                "gds.similarity.jaccard" | "jaccard" => {
                    let a = self.eval_arg(args, 0, binding)?;
                    let b = self.eval_arg(args, 1, binding)?;
                    match (a, b) {
                        (Value::List(la), Value::List(lb)) => {
                            let set_a: std::collections::HashSet<String> =
                                la.iter().map(|v| format!("{:?}", v)).collect();
                            let set_b: std::collections::HashSet<String> =
                                lb.iter().map(|v| format!("{:?}", v)).collect();
                            let intersection = set_a.intersection(&set_b).count() as f64;
                            let union = set_a.union(&set_b).count() as f64;
                            if union == 0.0 {
                                Ok(Value::Float(0.0))
                            } else {
                                Ok(Value::Float(intersection / union))
                            }
                        }
                        _ => Ok(Value::Null),
                    }
                }
                "gds.similarity.overlap" | "overlap" => {
                    let a = self.eval_arg(args, 0, binding)?;
                    let b = self.eval_arg(args, 1, binding)?;
                    match (a, b) {
                        (Value::List(la), Value::List(lb)) => {
                            let set_a: std::collections::HashSet<String> =
                                la.iter().map(|v| format!("{:?}", v)).collect();
                            let set_b: std::collections::HashSet<String> =
                                lb.iter().map(|v| format!("{:?}", v)).collect();
                            let intersection = set_a.intersection(&set_b).count() as f64;
                            let min_size = (set_a.len().min(set_b.len())) as f64;
                            if min_size == 0.0 {
                                Ok(Value::Float(0.0))
                            } else {
                                Ok(Value::Float(intersection / min_size))
                            }
                        }
                        _ => Ok(Value::Null),
                    }
                }
                "gds.similarity.pearson" | "pearson" => {
                    let a = self.eval_arg(args, 0, binding)?;
                    let b = self.eval_arg(args, 1, binding)?;
                    match (a, b) {
                        (Value::List(la), Value::List(lb))
                            if la.len() == lb.len() && !la.is_empty() =>
                        {
                            let va: Vec<f64> = la
                                .iter()
                                .filter_map(|v| match v {
                                    Value::Float(f) => Some(*f),
                                    Value::Int(i) => Some(*i as f64),
                                    _ => None,
                                })
                                .collect();
                            let vb: Vec<f64> = lb
                                .iter()
                                .filter_map(|v| match v {
                                    Value::Float(f) => Some(*f),
                                    Value::Int(i) => Some(*i as f64),
                                    _ => None,
                                })
                                .collect();
                            if va.len() != vb.len() || va.is_empty() {
                                return Ok(Value::Null);
                            }
                            let n = va.len() as f64;
                            let mean_a = va.iter().sum::<f64>() / n;
                            let mean_b = vb.iter().sum::<f64>() / n;
                            let cov: f64 = va
                                .iter()
                                .zip(&vb)
                                .map(|(a, b)| (a - mean_a) * (b - mean_b))
                                .sum();
                            let std_a: f64 =
                                va.iter().map(|a| (a - mean_a).powi(2)).sum::<f64>().sqrt();
                            let std_b: f64 =
                                vb.iter().map(|b| (b - mean_b).powi(2)).sum::<f64>().sqrt();
                            if std_a == 0.0 || std_b == 0.0 {
                                Ok(Value::Float(0.0))
                            } else {
                                Ok(Value::Float(cov / (std_a * std_b)))
                            }
                        }
                        _ => Ok(Value::Null),
                    }
                }
                // GDS utility functions
                "gds.util.nan" => Ok(Value::Float(f64::NAN)),
                "gds.util.infinity" => Ok(Value::Float(f64::INFINITY)),
                "gds.util.isfinite" => {
                    let v = self.eval_arg(args, 0, binding)?;
                    match v {
                        Value::Float(f) => Ok(Value::Bool(f.is_finite())),
                        Value::Int(_) => Ok(Value::Bool(true)),
                        Value::Null => Ok(Value::Bool(false)),
                        _ => Ok(Value::Bool(false)),
                    }
                }
                "gds.util.isinfinite" => {
                    let v = self.eval_arg(args, 0, binding)?;
                    match v {
                        Value::Float(f) => Ok(Value::Bool(!f.is_finite())),
                        Value::Int(_) => Ok(Value::Bool(false)),
                        Value::Null => Ok(Value::Bool(true)),
                        _ => Ok(Value::Bool(true)),
                    }
                }
                "gds.util.asnode" => {
                    let v = self.eval_arg(args, 0, binding)?;
                    if let Value::Str(id) = v {
                        for (_, val) in binding.iter() {
                            if let Value::Node(n) = val {
                                if n.id == id {
                                    return Ok(Value::Node(n.clone()));
                                }
                            }
                        }
                    }
                    Ok(Value::Null)
                }
                "gds.util.asnodes" => {
                    let v = self.eval_arg(args, 0, binding)?;
                    if let Value::List(ids) = v {
                        let nodes: Vec<Value> = ids
                            .into_iter()
                            .filter_map(|id| {
                                if let Value::Str(ref s) = id {
                                    for (_, val) in binding.iter() {
                                        if let Value::Node(n) = val {
                                            if &n.id == s {
                                                return Some(Value::Node(n.clone()));
                                            }
                                        }
                                    }
                                }
                                None
                            })
                            .collect();
                        Ok(Value::List(nodes))
                    } else {
                        Ok(Value::Null)
                    }
                }
                "gds.util.nodepropertyname" => {
                    let v = self.eval_arg(args, 0, binding)?;
                    Ok(v)
                }
                // GDS link prediction (require graph context — return null in scalar eval)
                "gds.alpha.linkprediction.adamicadar" | "adamic_adar" => Ok(Value::Null),
                "gds.alpha.linkprediction.commonneighbors" | "common_neighbors" => {
                    Ok(Value::Null)
                }
                "gds.alpha.linkprediction.preferentialattachment"
                | "preferential_attachment" => Ok(Value::Null),
                "gds.alpha.linkprediction.resourceallocation" | "resource_allocation" => {
                    Ok(Value::Null)
                }
                "gds.alpha.linkprediction.totalneighbors" | "total_neighbors" => {
                    Ok(Value::Null)
                }
                "gds.alpha.linkprediction.samecommunity" | "same_community" => {
                    Ok(Value::Null)
                }
                // count_distinct is handled in aggregation; if used in non-agg context, count unique
                "count_distinct" => {
                    let v = self.eval_arg(args, 0, binding)?;
                    Ok(Value::Int(if matches!(v, Value::Null) { 0 } else { 1 }))
                }
                other => {
                    // Unknown function — return null rather than error to be lenient
                    let _ = other;
                    Ok(Value::Null)
                }
            }
        }

        fn numeric_fn(&self, v: Value, f: impl Fn(f64) -> f64) -> Result<Value> {
            match v {
                Value::Int(n) => Ok(Value::Float(f(n as f64))),
                Value::Float(fl) => Ok(Value::Float(f(fl))),
                Value::Null => Ok(Value::Null),
                _ => Err(CypherError::TypeError("function requires numeric argument".into())),
            }
        }

        fn eval_arg(&self, args: &[Expr], idx: usize, binding: &Binding) -> Result<Value> {
            let expr = args
                .get(idx)
                .ok_or_else(|| CypherError::TypeError(format!("missing argument {}", idx)))?;
            self.eval(expr, binding)
        }
    }

    impl Default for Executor {
        fn default() -> Self {
            Self::new()
        }
    }
}

#[cfg(test)]
mod tests;
