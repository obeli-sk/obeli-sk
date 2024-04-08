use concepts::{FunctionFqn, StrVariant};
use std::{collections::HashMap, error::Error, ops::Deref};
use tracing::{debug, info, trace};
use utils::wasm_tools;
use wit_parser::{Resolve, WorldId};

mod workflow_ctx;

pub struct EngineConfig {
    pub allocation_strategy: wasmtime::InstanceAllocationStrategy,
}

impl Default for EngineConfig {
    fn default() -> Self {
        Self {
            allocation_strategy: wasmtime::InstanceAllocationStrategy::pooling(),
        }
    }
}

#[derive(thiserror::Error, Debug)]
pub enum WasmFileError {
    #[error("cannot open `{0}` - {1}")]
    CannotOpen(StrVariant, std::io::Error),
    #[error("cannot decode `{0}` - {1}")]
    DecodeError(StrVariant, wasm_tools::DecodeError),
    #[error("cannot decode metadata `{0}` - {1}")]
    FunctionMetadataError(StrVariant, wasm_tools::FunctionMetadataError),
    #[error("cannot link `{file}` - {reason}, details: {err}")]
    LinkingError {
        file: StrVariant,
        reason: StrVariant,
        err: Box<dyn Error>,
    },
    #[error("cannot compile `{0}` - {1}")]
    CompilationError(StrVariant, Box<dyn Error>),
}

#[derive(derivative::Derivative)]
#[derivative(Debug)]
struct WasmComponent {
    wasm_path: StrVariant,
    #[derivative(Debug = "ignore")]
    wasm: Vec<u8>,
    exported_ffqns_to_results_len: HashMap<FunctionFqn, usize>,
    imported_ifc_fns: Vec<wasm_tools::PackageIfcFns>,
    resolve: Resolve,
    world_id: WorldId,
}

impl WasmComponent {
    fn new(wasm_path: StrVariant) -> Result<Self, WasmFileError> {
        info!("Reading");
        let wasm = std::fs::read(wasm_path.deref())
            .map_err(|err| WasmFileError::CannotOpen(wasm_path.clone(), err))?;
        let (resolve, world_id) = wasm_tools::decode(&wasm)
            .map_err(|err| WasmFileError::DecodeError(wasm_path.clone(), err))?;
        let exported_ffqns_to_results_len = {
            let exported_interfaces = wasm_tools::exported_ifc_fns(&resolve, &world_id)
                .map_err(|err| WasmFileError::DecodeError(wasm_path.clone(), err))?;
            trace!("Exported functions: {exported_interfaces:?}");
            wasm_tools::functions_and_result_lengths(exported_interfaces)
                .map_err(|err| WasmFileError::FunctionMetadataError(wasm_path.clone(), err))?
        };
        debug!(?exported_ffqns_to_results_len, "Exported functions");
        let imported_ifc_fns = wasm_tools::imported_ifc_fns(&resolve, &world_id)
            .map_err(|err| WasmFileError::DecodeError(wasm_path.clone(), err))?;
        trace!("Imported interfaces: {imported_ifc_fns:?}");
        Ok(Self {
            wasm_path,
            wasm,
            exported_ffqns_to_results_len,
            imported_ifc_fns,
            resolve,
            world_id,
        })
    }
}
