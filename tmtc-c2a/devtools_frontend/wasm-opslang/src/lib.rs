use opslang_syn::typedef::*;
use wasm_bindgen::prelude::*;
use web_sys::js_sys;

mod free_variables;
mod union_value;
use union_value::UnionValue;

#[wasm_bindgen]
pub fn set_panic_hook() {
    #[cfg(feature = "console_error_panic_hook")]
    console_error_panic_hook::set_once();
}

macro_rules! log {
    ( $( $t:tt )* ) => {
        web_sys::console::log_1(&format!( $( $t )* ).into());
    }
}

#[derive(Debug)]
enum RuntimeError {
    ParseIntError(std::num::ParseIntError),
    ParseFloatError(std::num::ParseFloatError),
    Unimplemented(&'static str),
    TypeError(&'static str, &'static str),
    NoOverload(&'static str, &'static str, &'static str),
    CheckValueFailure,
    JsOriginError(JsValue),
    Other(String),
    DivideByZero,
}

type Result<T, E = RuntimeError> = std::result::Result<T, E>;

#[wasm_bindgen(typescript_custom_section)]
const TS_SECTION_DRIVER: &str = r#"
interface Driver{
    sendCommand(
      prefix: string,
      component: string,
      executingComponent: string | undefined,
      timeIndicator: Value | undefined,
      command: string,
      params: Value[]
    ) : Promise<void>;
    resolveVariable(variablePath : string) : Value | undefined;
    setLocalVariable(ident : string, value : Value);
    print(value : Value) : Promise<void>;
}
"#;

#[wasm_bindgen]
extern "C" {
    #[wasm_bindgen(js_name = "Driver", typescript_type = "Driver")]
    pub type Driver;

    #[wasm_bindgen(catch, method, js_name = "sendCommand")]
    pub async fn send_command(
        this: &Driver,
        prefix: &str,
        component: &str,
        executingComponent: Option<&str>,
        time_indicator: Option<UnionValue>,
        command_name: &str,
        args: Vec<UnionValue>,
    ) -> Result<(), JsValue>;

    // ここをasyncにすると評価がasync再帰になってちょっと面倒
    // スタックマシンにするか？
    #[wasm_bindgen(method, js_name = "resolveVariable")]
    pub fn resolve_variable(this: &Driver, variable_path: &str) -> Option<UnionValue>;

    // mutableな状態管理はExecutor側に任せることにする
    #[wasm_bindgen(method, js_name = "setLocalVariable")]
    pub fn set_local_variable(this: &Driver, ident: &str, value: UnionValue);

    #[wasm_bindgen(catch, method, js_name = "print")]
    pub async fn print(this: &Driver, value: UnionValue) -> Result<(), JsValue>;
}

#[derive(Debug)]
enum Value {
    Integer(i64),
    Double(f64),
    Bool(bool),
    Array(Vec<Value>),
    String(String),
    Duration(chrono::Duration),
}

impl From<i64> for Value {
    fn from(v: i64) -> Self {
        Value::Integer(v)
    }
}

impl From<f64> for Value {
    fn from(v: f64) -> Self {
        Value::Double(v)
    }
}

impl From<bool> for Value {
    fn from(v: bool) -> Self {
        Value::Bool(v)
    }
}

impl From<chrono::Duration> for Value {
    fn from(v: chrono::Duration) -> Self {
        Value::Duration(v)
    }
}

impl Value {
    fn type_name(&self) -> &'static str {
        use Value::*;
        match self {
            Integer(_) => "integer",
            Double(_) => "double",
            Bool(_) => "bool",
            Array(_) => "array",
            String(_) => "string",
            Duration(_) => "duration",
        }
    }

    fn integer(&self) -> Result<i64> {
        self.try_into()
    }

    fn double(&self) -> Result<f64> {
        self.try_into()
    }

    fn bool(&self) -> Result<bool> {
        self.try_into()
    }

    fn array(&self) -> Result<&Vec<Value>> {
        match self {
            Value::Array(x) => Ok(x),
            _ => type_err("array", self),
        }
    }

    fn string(&self) -> Result<&str> {
        match self {
            Value::String(x) => Ok(x),
            _ => type_err("string", self),
        }
    }

    fn duration(&self) -> Result<chrono::Duration> {
        self.try_into()
    }
}

impl<'a> TryInto<i64> for &'a Value {
    type Error = RuntimeError;
    fn try_into(self) -> Result<i64> {
        match self {
            Value::Integer(x) => Ok(*x),
            _ => type_err("integer", self),
        }
    }
}

impl<'a> TryInto<f64> for &'a Value {
    type Error = RuntimeError;
    fn try_into(self) -> Result<f64> {
        match self {
            Value::Double(x) => Ok(*x),
            _ => type_err("double", self),
        }
    }
}

impl TryInto<bool> for &Value {
    type Error = RuntimeError;
    fn try_into(self) -> Result<bool> {
        match self {
            Value::Bool(x) => Ok(*x),
            _ => type_err("bool", self),
        }
    }
}

impl TryInto<chrono::Duration> for &Value {
    type Error = RuntimeError;
    fn try_into(self) -> Result<chrono::Duration> {
        match self {
            Value::Duration(x) => Ok(*x),
            _ => type_err("duration", self),
        }
    }
}

fn type_err<T>(expected: &'static str, e: &Value) -> Result<T> {
    Err(RuntimeError::TypeError(expected, e.type_name()))
}

fn unimpl<T>(s: &'static str) -> Result<T> {
    Err(RuntimeError::Unimplemented(s))
}

enum BinopChain {
    Unresolved(Value, Value),
    Resolved(Value),
}

impl BinopChain {
    fn new(l: Value, r: Value) -> Self {
        BinopChain::Unresolved(l, r)
    }

    fn or<L, R>(self, f: impl Fn(L, R) -> Result<Value>) -> Result<Self>
    where
        for<'a> &'a Value: TryInto<L>,
        for<'a> &'a Value: TryInto<R>,
    {
        match &self {
            BinopChain::Unresolved(l, r) => {
                let l_cast = (l).try_into();
                let r_cast = (r).try_into();
                if let (Ok(l), Ok(r)) = (l_cast, r_cast) {
                    return f(l, r).map(BinopChain::Resolved);
                }
                Ok(self)
            }
            BinopChain::Resolved(_) => Ok(self),
        }
    }

    fn unwrap(self, name: &'static str) -> Result<Value> {
        match self {
            BinopChain::Unresolved(l, r) => {
                Err(RuntimeError::NoOverload(name, l.type_name(), r.type_name()))
            }
            BinopChain::Resolved(v) => Ok(v),
        }
    }
}

struct Runner {
    driver: Driver,
}

impl Runner {
    pub(crate) fn expr(&self, e: &Expr) -> Result<Value> {
        use Expr::*;
        match e {
            Variable(variable_path) => self.variable(variable_path),
            TlmRef(tlm_id) => self.tlmref(tlm_id),
            Literal(l) => self.literal(l),
            UnOp(unop, e) => self.unop(unop, e),
            BinOp(binop, left, right) => self.binop(binop, left, right),
            FunCall(_fun, _args) => unimpl("expr.funcall"),
        }
    }

    pub fn wait_expr(
        &self,
        e: &Expr,
        evaluated_durations: &mut Vec<Option<chrono::Duration>>,
        position: usize,
        elapsed_time: chrono::Duration,
    ) -> Result<(bool, usize)> {
        use BinOpKind::*;
        use Expr::*;
        match e {
            BinOp(And, left, right) => {
                let (left, next_position) =
                    self.wait_expr(left, evaluated_durations, position, elapsed_time)?;
                let (right, next_position) =
                    self.wait_expr(right, evaluated_durations, next_position, elapsed_time)?;
                Ok((left && right, next_position))
            }
            BinOp(Or, left, right) => {
                let (left, next_position) =
                    self.wait_expr(left, evaluated_durations, position, elapsed_time)?;
                let (right, next_position) =
                    self.wait_expr(right, evaluated_durations, next_position, elapsed_time)?;
                Ok((left || right, next_position))
            }
            BinOp(If, left, right) => {
                let (left, next_position) =
                    self.wait_expr(left, evaluated_durations, position, elapsed_time)?;
                let (right, next_position) =
                    self.wait_expr(right, evaluated_durations, next_position, elapsed_time)?;
                Ok((left || !right, next_position))
            }

            _ => {
                if evaluated_durations.len() <= position {
                    evaluated_durations.resize(position + 1, None);
                }
                let next_position = position + 1;

                if let Some(duration) = evaluated_durations[position] {
                    return Ok((duration <= elapsed_time, next_position));
                }

                let v = self.expr(e)?;
                match v {
                    Value::Bool(b) => Ok((b, next_position)),
                    Value::Duration(d) => {
                        evaluated_durations[position] = Some(d);
                        Ok((d <= elapsed_time, next_position))
                    }
                    _ => Err(RuntimeError::TypeError("bool or duration", v.type_name())),
                }
            }
        }
    }

    pub fn variable(&self, variable_path: &VariablePath) -> Result<Value> {
        self.driver
            .resolve_variable(&variable_path.raw)
            .map(Into::into)
            .ok_or_else(|| RuntimeError::Other(format!("variable {} not found", variable_path.raw)))
    }

    pub fn tlmref(&self, variable_path: &VariablePath) -> Result<Value> {
        //FIXME: prefixing with "$" is a dirty hack
        self.driver
            .resolve_variable(format!("${}", variable_path.raw).as_str())
            .map(Into::into)
            .ok_or_else(|| RuntimeError::Other(format!("variable {} not found", variable_path.raw)))
    }

    fn literal(&self, l: &Literal) -> Result<Value> {
        use Literal::*;
        match l {
            Array(es) => es
                .iter()
                .map(|e| self.expr(e))
                .collect::<Result<_, _>>()
                .map(Value::Array),
            Numeric(num, s) => self.numeric(num, s),
            String(s) => Ok(Value::String((*s).to_owned())),
            DateTime(_dt) => unimpl("expr.datetime"),
            TlmId(_tlm_id) => unimpl("expr.tlm_id"),
        }
    }

    fn numeric(&self, num: &Numeric, s: &Option<NumericSuffix>) -> Result<Value> {
        use Numeric::*;
        match num {
            Integer(nums, prefix) => {
                use IntegerPrefix::*;
                let base = match prefix {
                    Hexadecimal => 16,
                    Decimal => 10,
                    Octal => 8,
                    Binary => 2,
                };
                let v = i64::from_str_radix(nums, base).map_err(RuntimeError::ParseIntError)?;

                match s {
                    Some(NumericSuffix::Second) => {
                        let v = chrono::Duration::seconds(v);
                        Ok(Value::Duration(v))
                    }
                    None => Ok(Value::Integer(v)),
                }
            }
            Float(nums) => {
                let v: f64 = nums.parse().map_err(RuntimeError::ParseFloatError)?;

                match s {
                    Some(NumericSuffix::Second) => {
                        let millis = (v * 1000.0) as i64;
                        let v = chrono::Duration::milliseconds(millis);
                        Ok(Value::Duration(v))
                    }
                    None => Ok(Value::Double(v)),
                }
            }
        }
    }

    fn binop(&self, op: &BinOpKind, left: &Expr, right: &Expr) -> Result<Value> {
        use BinOpKind::*;
        match op {
            Compare(comp_op) => self.compare(comp_op, left, right),
            If => self.bool_binop(|x, y| x || !y, left, right),
            And => self.bool_binop(bool::min, left, right),
            Or => self.bool_binop(bool::max, left, right),
            Mul => BinopChain::new(self.expr(left)?, self.expr(right)?)
                .or(|x: i64, y: i64| Ok((x * y).into()))?
                .or(|x: f64, y: f64| Ok((x * y).into()))?
                //TODO: safer cast i64 -> i32
                .or(|x: chrono::Duration, y: i64| Ok((x * (y as i32)).into()))?
                .or(|x: i64, y: chrono::Duration| Ok((y * (x as i32)).into()))?
                .unwrap("mul"),
            Div => BinopChain::new(self.expr(left)?, self.expr(right)?)
                .or(|x: i64, y: i64| {
                    if y == 0 {
                        Err(RuntimeError::DivideByZero)
                    } else {
                        Ok((x / y).into())
                    }
                })?
                .or(|x: f64, y: f64| Ok((x / y).into()))?
                .or(|x: chrono::Duration, y: i64| {
                    if y == 0 {
                        Err(RuntimeError::DivideByZero)
                    } else {
                        Ok((x / (y as i32)).into())
                    }
                })?
                .unwrap("div"),
            Add => BinopChain::new(self.expr(left)?, self.expr(right)?)
                .or(|x: i64, y: i64| Ok((x + y).into()))?
                .or(|x: f64, y: f64| Ok((x + y).into()))?
                .or(|x: chrono::Duration, y: chrono::Duration| Ok((x + y).into()))?
                .unwrap("add"),
            Sub => BinopChain::new(self.expr(left)?, self.expr(right)?)
                .or(|x: i64, y: i64| Ok((x + y).into()))?
                .or(|x: f64, y: f64| Ok((x + y).into()))?
                .or(|x: chrono::Duration, y: chrono::Duration| Ok((x + y).into()))?
                .unwrap("sub"),
            Mod => {
                let left = self.expr(left)?.integer()?;
                let right = self.expr(right)?.integer()?;
                if right == 0 {
                    Err(RuntimeError::DivideByZero)
                } else {
                    Ok((left % right).into())
                }
            }
            In => {
                let left = self.expr(left)?;
                let right = self.expr(right)?;
                let right = right.array()?;
                if right.len() != 2 {
                    return Err(RuntimeError::Other(
                        "the second operand must have two elements".to_owned(),
                    ));
                }
                use Value::*;
                match left {
                    Integer(x) => {
                        let start = right[0].integer()?;
                        let end = right[1].integer()?;
                        Ok((start <= x && x <= end).into())
                    }
                    Double(x) => {
                        let start = right[0].double()?;
                        let end = right[1].double()?;
                        Ok((start <= x && x <= end).into())
                    }
                    Bool(x) => {
                        let start = right[0].bool()?;
                        let end = right[1].bool()?;
                        Ok((start <= x && x <= end).into())
                    }
                    _ => type_err("comparable", &left),
                }
            }
        }
    }

    // add short-circuit?
    // short-circuitを入れると右辺の型がおかしくても通してしまう
    fn bool_binop(
        &self,
        op: impl Fn(bool, bool) -> bool,
        left: &Expr,
        right: &Expr,
    ) -> Result<Value> {
        let left = self.expr(left)?.bool()?;
        let right = self.expr(right)?.bool()?;
        Ok(op(left, right).into())
    }

    fn unop(&self, op: &UnOpKind, e: &Expr) -> Result<Value> {
        use UnOpKind::*;
        use Value::*;
        match op {
            Neg => {
                let v = self.expr(e)?;
                match v {
                    Integer(x) => Ok(Integer(-x)),
                    Double(x) => Ok(Double(-x)),
                    Bool(x) => Ok(Bool(!x)),
                    Duration(x) => Ok(Duration(-x)),
                    Array(_) | String(_) => type_err("numeric or bool", &v),
                }
            }
        }
    }

    fn compare(&self, comp_op: &CompareBinOpKind, left: &Expr, right: &Expr) -> Result<Value> {
        let left = self.expr(left)?;
        let right = self.expr(right)?;

        use Value::*;
        let ord = match left {
            Integer(x) => Some(x.cmp(&right.integer()?)),
            Double(x) => x.partial_cmp(&right.double()?),
            Bool(x) => Some(x.cmp(&right.bool()?)),
            Array(_) => return type_err("comparable", &left),
            String(x) => Some(x[..].cmp(right.string()?)),
            Duration(x) => Some(x.cmp(&right.duration()?)),
        };
        let ord = match ord {
            Some(ord) => ord,
            None => return Ok(false.into()),
        };

        use std::cmp::Ordering;
        use CompareBinOpKind::*;
        let b = match comp_op {
            GreaterEq => ord >= Ordering::Equal,
            LessEq => ord <= Ordering::Equal,
            Greater => ord == Ordering::Greater,
            Less => ord == Ordering::Less,
            NotEqual => ord != Ordering::Equal,
            Equal => ord == Ordering::Equal,
        };
        Ok(b.into())
    }

    async fn send_command(
        &self,
        prefix: &str,
        component: &str,
        executing_component: Option<&str>,
        time_indicator: Option<UnionValue>,
        command_name: &str,
        args: Vec<UnionValue>,
    ) -> Result<()> {
        self.driver
            .send_command(
                prefix,
                component,
                executing_component,
                time_indicator,
                command_name,
                args,
            )
            .await
            .map_err(RuntimeError::JsOriginError)
    }

    async fn exec_statement<'bc>(
        &mut self,
        block_context: BlockContext<'bc>,
        mut context: ExecutionContext,
        stmt: &SingleStatement,
        current_time_ms: usize,
    ) -> Result<ExecutionResult> {
        use SingleStatement::*;
        match stmt {
            Call(_) => unimpl("stmt.call"),
            Wait(c) => {
                // Wait文の条件式として有効なものは以下の3条件によって帰納的に定義される
                // 1. bool型に評価される式であって、式のトップレベルの構成子が二項論理演算子でないものは有効
                // 2. duration型に評価される式は有効
                // (以上1.2.をAtomic条件式と呼ぶことにする)
                // 3. E1, E2が共に有効なら、E1とE2を二項論理演算子で繋いだものも有効
                //
                //
                // duration型に評価されるAtomic条件式については、初回呼び出しの際の評価値が記録され、
                // 「初回呼び出しからの経過時間」が「初回呼び出しの際の評価値」を超える場合に真と評価される
                // 「初回呼び出しの際の評価値」はExecutionContextのevaluated_durationsに記録され、
                // そのindexは左から何番目のAtomic条件式であるかを表す
                // 例えば、
                //                  1s < 2s && 3s &&  ("A" == "B" || 4s)
                // を評価するとevaluated_durationsには
                //                 [None, Some(3s),    None,    Some(4s)]
                // が記録される
                // この時 1s < 2s は bool型に評価されるAtomic条件式であってduration型には評価されないことに注意

                //FIXME: what if current_time_ms < context.initial_execution_time_ms?
                let (cond, _) = self.wait_expr(
                    &c.condition,
                    &mut context.evaluated_durations,
                    0,
                    chrono::Duration::milliseconds(
                        (current_time_ms - context.initial_execution_time_ms) as _,
                    ),
                )?;
                if cond {
                    Ok(ExecutionResult::executed())
                } else {
                    Ok(ExecutionResult::retry(context))
                }
            }
            Assert(c) => {
                let cond = self.expr(&c.condition)?;
                match cond {
                    Value::Bool(true) => Ok(ExecutionResult::executed()),
                    Value::Bool(false) => Err(RuntimeError::CheckValueFailure),
                    _ => Err(RuntimeError::TypeError("bool", cond.type_name())),
                }
            }
            AssertEq(_a) => unimpl("stmt.assert_eq"),
            Command(command) => {
                let receiver = command
                    .destination
                    .receiver
                    .as_ref()
                    .or(block_context.default_destination)
                    .ok_or_else(|| RuntimeError::Other("no receiver".to_owned()))?;
                let executor = command.destination.executor.as_ref();
                let ti = if let Some(ti) = &command.destination.time_indicator {
                    Some(self.expr(ti)?.into())
                } else {
                    None
                };

                let args: Result<Vec<_>> = command
                    .args
                    .iter()
                    .map(|e| self.expr(e).map(Into::into))
                    .collect();

                self.send_command(
                    receiver.exec_method.as_str(),
                    receiver.component.as_str(),
                    executor.map(|e| e.component.as_str()),
                    ti,
                    &command.name,
                    args?,
                )
                .await?;
                //TODO: apply delay
                Ok(ExecutionResult::executed())
            }
            Let(l) => {
                let value = self.expr(&l.rhs)?;
                self.driver
                    .set_local_variable(&l.variable.raw, value.into());
                Ok(ExecutionResult::executed())
            }
            Print(p) => {
                let arg = self.expr(&p.arg)?;
                self.driver
                    .print(arg.into())
                    .await
                    .map_err(RuntimeError::JsOriginError)?;
                Ok(ExecutionResult::executed())
            }
        }
    }
}

#[wasm_bindgen]
#[derive(Debug, Clone, Copy, PartialOrd, Ord, PartialEq, Eq)]
pub enum ControlStatus {
    // Stopped at a breakpoint
    // Executor (i.e. the caller of `execute_line`) should stop execution, and execute this line
    // again when resuming execution
    Breaked,

    // Executor shold proceed to the next line
    Executed,

    // Wait condition is not satisfied
    // Executor should execute this line again
    Retry,
}

#[wasm_bindgen]
#[derive(Debug, Clone)]
pub struct ExecutionContext {
    initial_execution_time_ms: usize,
    evaluated_durations: Vec<Option<chrono::Duration>>,
}

#[wasm_bindgen]
#[derive(Debug)]
pub struct ExecutionResult {
    pub status: ControlStatus,
    execution_context: Option<ExecutionContext>,
}

#[wasm_bindgen]
impl ExecutionResult {
    #[wasm_bindgen(getter)]
    pub fn execution_context(&self) -> Option<ExecutionContext> {
        self.execution_context.clone()
    }
}

impl ExecutionResult {
    fn breaked() -> Self {
        ExecutionResult {
            status: ControlStatus::Breaked,
            execution_context: None,
        }
    }

    fn executed() -> Self {
        ExecutionResult {
            status: ControlStatus::Executed,
            execution_context: None,
        }
    }

    fn retry(execution_context: ExecutionContext) -> Self {
        ExecutionResult {
            status: ControlStatus::Retry,
            execution_context: Some(execution_context),
        }
    }
}

//TODO: reimplement this
#[wasm_bindgen(js_name = validateLine)]
pub fn validate_line(_input: &str, _line_num: usize) -> Result<(), String> {
    Ok(())
    //opslang_syn::parser::parse_row(input)
    //    .map(|_| ())
    //    .map_err(|mut e| {
    //        e.location.line += line_num;
    //        e.location.line -= 1; // because line numbers are 1-indexed
    //        e.to_string()
    //    })
}

#[wasm_bindgen]
pub struct ParsedCode {
    ast: Vec<Statement>,
    line_offsets: Vec<usize>,
}

struct BlockContext<'a> {
    default_destination: Option<&'a Destination>,
    delay: Option<&'a Expr>,
}

enum FoundRow<'a> {
    RowWithContext {
        block_context: BlockContext<'a>,
        row: &'a Row,
    },
    Empty, // found, but empty (e.g. opening/closisng brace)
}

impl ParsedCode {
    fn find_row(&self, line_num: usize) -> Option<FoundRow> {
        let offset = self.line_offsets[line_num - 1];
        let statement = self.ast.iter().find(|stmt| {
            let span = match stmt {
                Statement::Single(row) => &row.span,
                Statement::Block(block) => &block.span,
            };
            span.contains(&offset)
        })?;
        match statement {
            Statement::Single(row) => Some(FoundRow::RowWithContext {
                block_context: BlockContext {
                    default_destination: None,
                    delay: None,
                },
                row,
            }),
            Statement::Block(block) => {
                if block.rows.is_empty()
                    || offset < block.rows[0].span.start
                    || offset > block.rows.last().unwrap().span.end
                {
                    Some(FoundRow::Empty)
                } else {
                    let row = block.rows.iter().find(|row| row.span.contains(&offset))?;
                    Some(FoundRow::RowWithContext {
                        block_context: BlockContext {
                            default_destination: block.default_destination.as_ref(),
                            delay: block.delay.as_ref(),
                        },
                        row,
                    })
                }
            }
        }
        //self.ast.iter().find(|row| row.span.contains(&offset))
    }
}

#[wasm_bindgen]
impl ParsedCode {
    #[wasm_bindgen(js_name = fromCode)]
    pub fn from_code(code: &str) -> Result<ParsedCode, JsValue> {
        let ast = opslang_syn::parser::parse_statements(code).map_err(|e| e.to_string())?;
        let newlines = code
            .char_indices()
            .filter(|(_, c)| *c == '\n')
            .map(|(i, _)| i);
        let line_offsets = std::iter::once(0).chain(newlines.map(|i| i + 1)).collect();
        Ok(ParsedCode { ast, line_offsets })
    }

    #[wasm_bindgen(js_name = executeLine)]
    pub async fn execute_line(
        &self,
        driver: Driver,
        context: Option<ExecutionContext>,
        stop_on_break: bool,
        line_num: usize,
        current_time_ms: usize,
    ) -> Result<ExecutionResult, String> {
        let context = context.unwrap_or_else(|| ExecutionContext {
            initial_execution_time_ms: current_time_ms,
            evaluated_durations: vec![],
        });
        let result = self
            .execute_line_(driver, context, stop_on_break, line_num, current_time_ms)
            .await;
        match &result {
            Ok(sc) => {
                log!("execute_line ok: {:?}", sc);
            }
            Err(e) => {
                log!("execute_line err: {}", e);
            }
        };
        result
    }

    pub async fn execute_line_(
        &self,
        driver: Driver,
        context: ExecutionContext,
        stop_on_break: bool,
        line_num: usize,
        current_time_ms: usize,
    ) -> Result<ExecutionResult, String> {
        let mut runner = Runner { driver };

        let found_row = self
            .find_row(line_num)
            .ok_or_else(|| format!("line {} not found", line_num))?;

        let (block_context, row) = match found_row {
            FoundRow::Empty => return Ok(ExecutionResult::executed()),
            FoundRow::RowWithContext { block_context, row } => (block_context, row),
        };

        if row.breaks.is_some() && stop_on_break {
            return Ok(ExecutionResult::breaked());
        }
        if let Some(stmt) = &row.content {
            runner
                .exec_statement(block_context, context, stmt, current_time_ms)
                .await
                .map_err(|e| format!("{:?}", e))
        } else {
            Ok(ExecutionResult::executed())
        }
    }

    #[wasm_bindgen(js_name = freeVariables)]
    pub fn free_variables(&self, line_num: usize) -> Result<Vec<String>, String> {
        let found_row = self
            .find_row(line_num)
            .ok_or_else(|| format!("line {} not found", line_num))?;

        let row = match found_row {
            FoundRow::Empty => return Ok(vec![]),
            FoundRow::RowWithContext { row, .. } => row,
        };
        if let Some(stmt) = &row.content {
            use std::collections::HashSet;
            Ok(free_variables::stmt(
                stmt,
                &HashSet::new(), // TODO: manage bound variables?
            )
            .into_iter()
            .collect())
        } else {
            Ok(vec![])
        }
    }
}
