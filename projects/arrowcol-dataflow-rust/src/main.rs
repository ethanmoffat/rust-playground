use arrowcol_dataflow_components as _;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    arrowcol_dataflow_components::initialize_shared_io()?;
    otel_arrow_dfe::run()
}
