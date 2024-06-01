//! Worker that acts as wrapper for `activity_worker` or `workflow_worker`.
//! Apply following heuristic to distinguish between an activity and workflow:
//! * Read all imported functions of the component's world
//! * If there are no imports except for standard WASI -> Activity
//! * Otherwise -> Workflow

use crate::WasmFileError;
use concepts::{ComponentType, FunctionMetadata, IfcFqnName};
use std::{ops::Deref, path::Path};
use utils::wasm_tools::WasmComponent;
use wasmtime::Engine;

pub struct ComponentDetector {
    pub component_type: ComponentType,
    pub exports: Vec<FunctionMetadata>,
    pub imports: Vec<FunctionMetadata>,
}

impl ComponentDetector {
    #[must_use]
    pub fn get_engine() -> Engine {
        let mut wasmtime_config = wasmtime::Config::new();
        wasmtime_config.wasm_component_model(true);
        Engine::new(&wasmtime_config).unwrap()
    }

    pub fn new<P: AsRef<Path>>(wasm_path: P, engine: &Engine) -> Result<Self, WasmFileError> {
        let wasm_path = wasm_path.as_ref();
        let wasm_component = WasmComponent::new(wasm_path, engine)
            .map_err(|err| WasmFileError::DecodeError(wasm_path.to_owned(), err))?;
        let component_type =
            if supported_wasi_imports(wasm_component.exim.imports.iter().map(|pif| &pif.ifc_fqn)) {
                ComponentType::WasmActivity
            } else {
                ComponentType::WasmWorkflow
            };
        Ok(Self {
            component_type,
            exports: wasm_component.exported_functions().collect(),
            imports: wasm_component.imported_functions().collect(),
        })
    }
}

fn supported_wasi_imports<'a>(mut imported_packages: impl Iterator<Item = &'a IfcFqnName>) -> bool {
    // FIXME Fail if both wasi and host imports are present
    imported_packages.all(|ifc| ifc.deref().starts_with("wasi:"))
}

#[cfg(test)]
mod tests {
    use crate::component_detector::ComponentDetector;
    use crate::{workflow_worker::get_workflow_engine, EngineConfig};
    use concepts::ComponentType;

    use test_utils::set_up;

    #[rstest::rstest]
    #[case(
        test_programs_fibo_activity_builder::TEST_PROGRAMS_FIBO_ACTIVITY,
        ComponentType::WasmActivity
    )]
    #[case(
        test_programs_fibo_workflow_builder::TEST_PROGRAMS_FIBO_WORKFLOW,
        ComponentType::WasmWorkflow
    )]
    #[tokio::test]
    async fn detection(#[case] file: &'static str, #[case] expected: ComponentType) {
        set_up();
        let detected =
            ComponentDetector::new(file, &get_workflow_engine(EngineConfig::default())).unwrap();
        assert_eq!(expected, detected.component_type);
    }
}