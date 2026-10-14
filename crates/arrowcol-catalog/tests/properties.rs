use arrowcol_catalog::{DdlCommand, DdlParser};
use proptest::prelude::*;

proptest! {
    #[test]
    fn canonical_create_is_whitespace_and_keyword_case_independent(
        table in "[a-z][a-z0-9_]{0,12}",
        field in "[a-z][a-z0-9_]{0,12}",
    ) {
        let compact = format!("create table telemetry.prod.{table} ({field} bigint)");
        let spaced = format!(
            "  CREATE   TABLE telemetry.prod.{table} ( {field}   BIGINT  ) ; "
        );
        let DdlCommand::CreateTable(left) = DdlParser::default().parse(&compact)? else {
            return Err(TestCaseError::fail("expected create"));
        };
        let DdlCommand::CreateTable(right) = DdlParser::default().parse(&spaced)? else {
            return Err(TestCaseError::fail("expected create"));
        };
        prop_assert_eq!(left.canonical_ddl(), right.canonical_ddl());
        prop_assert_eq!(left.fields(), right.fields());
    }
}
