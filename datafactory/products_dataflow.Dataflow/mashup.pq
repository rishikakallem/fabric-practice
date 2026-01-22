[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "products csv_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared #"products csv" = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "20706a0b-f5cf-407c-8c0c-39a3c7e901d9"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "55426c38-4479-4246-a001-878ef1830557"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "Files", ItemKind = "Folder"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Name = "products.csv"]}[Content],
  #"Imported CSV" = Csv.Document(#"Navigation 3", [Delimiter = ",", Columns = 5, QuoteStyle = QuoteStyle.None]),
  #"Promoted headers" = Table.PromoteHeaders(#"Imported CSV", [PromoteAllScalars = true]),
  #"Changed column type" = Table.TransformColumnTypes(#"Promoted headers", {{"product_id", type text}, {"product_name", type text}, {"category", type text}, {"price", type text}, {"last_modified", type datetime}}),
  #"Filtered rows" = Table.SelectRows(#"Changed column type", each [product_id] <> null and [product_id] <> ""),
  #"Trimmed text" = Table.TransformColumns(#"Filtered rows", {{"product_id", each Text.Trim(_), type nullable text}, {"product_name", each Text.Trim(_), type nullable text}, {"category", each Text.Trim(_), type nullable text}}),
  #"Replaced value" = Table.ReplaceValue(#"Trimmed text", "O", "0", Replacer.ReplaceText, {"price"}),
  #"Changed column type with locale" = Table.TransformColumnTypes(#"Replaced value", {{"price", type number}}),
  #"Filtered rows 1" = Table.SelectRows(#"Changed column type with locale", each [price] >= 0),
  #"Replaced errors" = Table.ReplaceErrorValues(#"Filtered rows 1", {{"last_modified", null}})
in
  #"Replaced errors";
shared #"products csv_DataDestination" = let
  Pattern = Lakehouse.Contents([HierarchicalNavigation = null, CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "20706a0b-f5cf-407c-8c0c-39a3c7e901d9"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "55426c38-4479-4246-a001-878ef1830557"]}[Data],
  TableNavigation = Navigation_2{[Id = "silver_products", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
