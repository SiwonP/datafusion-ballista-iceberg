use datafusion::catalog::CatalogProvider;

#[derive(Debug)]
pub struct NessieCatalogProvider {

}

impl CatalogProvider for NessieCatalogProvider {
    fn as_any(&self) -> &dyn std::any::Any {
        todo!()
    }

    fn schema_names(&self) -> Vec<String> {
        todo!()
    }

    fn schema(&self, name: &str) -> Option<std::sync::Arc<dyn datafusion::catalog::SchemaProvider>> {
        todo!()
    }
    
    fn register_schema(
        &self,
        name: &str,
        schema: std::sync::Arc<dyn datafusion::catalog::SchemaProvider>,
    ) -> datafusion::error::Result<Option<std::sync::Arc<dyn datafusion::catalog::SchemaProvider>>> {
        // use variables to avoid unused variable warnings
        let _ = name;
        let _ = schema;
        datafusion::common::not_impl_err!("Registering new schemas is not supported")
    }
    
    fn deregister_schema(
        &self,
        _name: &str,
        _cascade: bool,
    ) -> datafusion::error::Result<Option<std::sync::Arc<dyn datafusion::catalog::SchemaProvider>>> {
        datafusion::common::not_impl_err!("Deregistering new schemas is not supported")
    }
}