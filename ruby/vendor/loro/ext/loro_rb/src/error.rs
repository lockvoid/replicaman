use magnus::{exception::ExceptionClass, prelude::*, value::Lazy, Error, RModule, Ruby};
use std::fmt::Display;

static LORO_ERROR: Lazy<ExceptionClass> = Lazy::new(|ruby| {
    let module: RModule = ruby.class_object().const_get("Loro").unwrap();
    module.const_get("Error").unwrap()
});

static IMPORT_ERROR: Lazy<ExceptionClass> = Lazy::new(|ruby| {
    let module: RModule = ruby.class_object().const_get("Loro").unwrap();
    module.const_get("ImportError").unwrap()
});

static TYPE_ERROR: Lazy<ExceptionClass> = Lazy::new(|ruby| {
    let module: RModule = ruby.class_object().const_get("Loro").unwrap();
    module.const_get("TypeError").unwrap()
});

pub fn loro(error: impl Display) -> Error {
    let ruby = Ruby::get().unwrap();
    Error::new(ruby.get_inner(&LORO_ERROR), error.to_string())
}

pub fn import(error: impl Display) -> Error {
    let ruby = Ruby::get().unwrap();
    Error::new(ruby.get_inner(&IMPORT_ERROR), error.to_string())
}

pub fn value_type(message: impl Into<String>) -> Error {
    let ruby = Ruby::get().unwrap();
    Error::new(ruby.get_inner(&TYPE_ERROR), message.into())
}
