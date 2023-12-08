use std::{borrow::Cow, fmt::Display};

use val_json::TypeWrapper;

pub mod activity;
pub mod event_history;
mod wasm_tools;
pub mod workflow;

#[derive(Hash, Clone, Debug, PartialEq, Eq)]
pub struct FunctionFqn<'a> {
    pub ifc_fqn: Cow<'a, str>,
    pub function_name: Cow<'a, str>,
}

impl FunctionFqn<'_> {
    pub fn new<'a>(ifc_fqn: &'a str, function_name: &'a str) -> FunctionFqn<'a> {
        FunctionFqn {
            ifc_fqn: Cow::Borrowed(ifc_fqn),
            function_name: Cow::Borrowed(function_name),
        }
    }
}

impl Display for FunctionFqn<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{ifc_fqn}.{function_name}",
            ifc_fqn = self.ifc_fqn,
            function_name = self.function_name
        )
    }
}

#[derive(Clone, Debug)]
pub struct FunctionMetadata {
    pub results_len: usize,
    // TODO pub results: Vec<TypeWrapper>,
    pub params: Vec<(String /*name*/, TypeWrapper)>,
}
