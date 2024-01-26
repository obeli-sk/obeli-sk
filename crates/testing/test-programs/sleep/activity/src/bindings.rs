// Generated by `wit-bindgen` 0.16.0. DO NOT EDIT!
pub mod exports {
  pub mod testing {
    pub mod sleep {
      
      #[allow(clippy::all)]
      pub mod sleep {
        #[used]
        #[doc(hidden)]
        #[cfg(target_arch = "wasm32")]
        static __FORCE_SECTION_REF: fn() = super::super::super::super::__link_section;
        const _: () = {
          
          #[doc(hidden)]
          #[export_name = "testing:sleep/sleep#sleep"]
          #[allow(non_snake_case)]
          unsafe extern "C" fn __export_sleep(arg0: i64,) {
            #[allow(unused_imports)]
            use wit_bindgen::rt::{alloc, vec::Vec, string::String};
            
            // Before executing any other code, use this function to run all static
            // constructors, if they have not yet been run. This is a hack required
            // to work around wasi-libc ctors calling import functions to initialize
            // the environment.
            //
            // This functionality will be removed once rust 1.69.0 is stable, at which
            // point wasi-libc will no longer have this behavior.
            //
            // See
            // https://github.com/bytecodealliance/preview2-prototyping/issues/99
            // for more details.
            #[cfg(target_arch="wasm32")]
            wit_bindgen::rt::run_ctors_once();
            
            <_GuestImpl as Guest>::sleep(arg0 as u64);
          }
        };
        use super::super::super::super::super::Component as _GuestImpl;
        pub trait Guest {
          fn sleep(millis: u64,);
        }
        
      }
      
    }
  }
  pub mod wasi {
    pub mod cli {
      
      #[allow(clippy::all)]
      pub mod run {
        #[used]
        #[doc(hidden)]
        #[cfg(target_arch = "wasm32")]
        static __FORCE_SECTION_REF: fn() = super::super::super::super::__link_section;
        const _: () = {
          
          #[doc(hidden)]
          #[export_name = "wasi:cli/run@0.2.0-rc-2023-12-05#run"]
          #[allow(non_snake_case)]
          unsafe extern "C" fn __export_run() -> i32 {
            #[allow(unused_imports)]
            use wit_bindgen::rt::{alloc, vec::Vec, string::String};
            
            // Before executing any other code, use this function to run all static
            // constructors, if they have not yet been run. This is a hack required
            // to work around wasi-libc ctors calling import functions to initialize
            // the environment.
            //
            // This functionality will be removed once rust 1.69.0 is stable, at which
            // point wasi-libc will no longer have this behavior.
            //
            // See
            // https://github.com/bytecodealliance/preview2-prototyping/issues/99
            // for more details.
            #[cfg(target_arch="wasm32")]
            wit_bindgen::rt::run_ctors_once();
            
            let result0 = <_GuestImpl as Guest>::run();
            let result1 = match result0 {
              Ok(_) => { 0i32 },
              Err(_) => { 1i32 },
            };result1
          }
        };
        use super::super::super::super::super::Component as _GuestImpl;
        pub trait Guest {
          /// Run the program.
          fn run() -> Result<(),()>;
        }
        
      }
      
    }
  }
}

#[cfg(target_arch = "wasm32")]
#[link_section = "component-type:any"]
#[doc(hidden)]
pub static __WIT_BINDGEN_COMPONENT_TYPE: [u8; 247] = [3, 0, 3, 97, 110, 121, 0, 97, 115, 109, 13, 0, 1, 0, 7, 129, 1, 1, 65, 2, 1, 65, 4, 1, 66, 2, 1, 64, 1, 6, 109, 105, 108, 108, 105, 115, 119, 1, 0, 4, 0, 5, 115, 108, 101, 101, 112, 1, 0, 4, 1, 19, 116, 101, 115, 116, 105, 110, 103, 58, 115, 108, 101, 101, 112, 47, 115, 108, 101, 101, 112, 5, 0, 1, 66, 3, 1, 106, 0, 0, 1, 64, 0, 0, 0, 4, 0, 3, 114, 117, 110, 1, 1, 4, 1, 32, 119, 97, 115, 105, 58, 99, 108, 105, 47, 114, 117, 110, 64, 48, 46, 50, 46, 48, 45, 114, 99, 45, 50, 48, 50, 51, 45, 49, 50, 45, 48, 53, 5, 1, 4, 1, 11, 97, 110, 121, 58, 97, 110, 121, 47, 97, 110, 121, 4, 0, 11, 9, 1, 0, 3, 97, 110, 121, 3, 0, 0, 0, 16, 12, 112, 97, 99, 107, 97, 103, 101, 45, 100, 111, 99, 115, 0, 123, 125, 0, 70, 9, 112, 114, 111, 100, 117, 99, 101, 114, 115, 1, 12, 112, 114, 111, 99, 101, 115, 115, 101, 100, 45, 98, 121, 2, 13, 119, 105, 116, 45, 99, 111, 109, 112, 111, 110, 101, 110, 116, 6, 48, 46, 49, 56, 46, 50, 16, 119, 105, 116, 45, 98, 105, 110, 100, 103, 101, 110, 45, 114, 117, 115, 116, 6, 48, 46, 49, 54, 46, 48];

#[inline(never)]
#[doc(hidden)]
#[cfg(target_arch = "wasm32")]
pub fn __link_section() {}
