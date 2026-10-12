use arrowcol_dataflow_components as _;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    otel_arrow_dfe::run()
}
