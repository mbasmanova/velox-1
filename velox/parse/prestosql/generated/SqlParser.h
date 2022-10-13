#ifndef SQLPARSER_H
#define SQLPARSER_H
#include "JavaCC.h"
#include "CharStream.h"
#include "Token.h"
#include "TokenManager.h"
#include "parser.h"
#include "SqlParserConstants.h"
#include "JJTSqlParserState.h"
#include "ErrorHandler.h"
#include "SqlParserTree.h"
namespace commonsql {
namespace parser {
  struct JJCalls {
    int        gen;
    int        arg;
    JJCalls*   next;
    Token*     first;
    ~JJCalls() { if (next) delete next; }
     JJCalls() { next = nullptr; arg = 0; gen = -1; first = nullptr; }
  };

class SqlParser {
public:
Node    * compilation_unit();
void statement_list();
void non_reserved_word();
void left_bracket_or_trigraph();
void right_bracket_or_trigraph();
void literal();
void signed_numeric_literal();
void unsigned_literal();
void unsigned_numeric_literal();
void exact_numeric_literal();
void general_literal();
void character_string_literal();
void Unicode_character_string_literal();
void datetime_literal();
void date_literal();
void time_literal();
void timestamp_literal();
void interval_literal();
void boolean_literal();
void identifier();
void actual_identifier();
void table_name();
void schema_name();
void catalog_name();
void schema_qualified_name();
void local_or_schema_qualified_name();
void local_or_schema_qualifier();
void cursor_name();
void local_qualifier();
void host_parameter_name();
void external_routine_name();
void character_set_name();
void schema_resolved_user_defined_type_name();
void user_defined_type_name();
void SQL_identifier();
void extended_identifier();
void dynamic_cursor_name();
void extended_cursor_name();
void descriptor_name();
void extended_descriptor_name();
void scope_option();
void data_type();
void predefined_type();
void character_string_type();
void character_large_object_type();
void national_character_string_type();
void national_character_large_object_type();
void binary_string_type();
void binary_large_object_string_type();
void numeric_type();
void exact_numeric_type();
void approximate_numeric_type();
void character_length();
void large_object_length();
void character_large_object_length();
void char_length_units();
void boolean_type();
void datetime_type();
void with_or_without_time_zone();
void interval_type();
void row_type();
void row_type_body();
void reference_type();
void scope_clause();
void referenced_type();
void path_resolved_user_defined_type_name();
void collection_type();
void array_type();
void multiset_type();
void field_definition();
void value_expression_primary();
void parenthesized_value_expression();
void nonparenthesized_value_expression_primary();
void primary_suffix();
void collection_value_constructor();
void value_specification();
void unsigned_value_specification();
void general_value_specification();
void simple_value_specification();
void target_specification();
void simple_target_specification();
void target_array_element_specification();
void current_collation_specification();
void contextually_typed_value_specification();
void implicitly_typed_value_specification();
void empty_specification();
void identifier_chain();
void column_reference();
void set_function_specification();
void grouping_operation();
void window_function();
void window_function_type();
void rank_function_type();
void ntile_function();
void number_of_tiles();
void lead_or_lag_function();
void lead_or_lag();
void null_treatment();
void first_or_last_value_function();
void first_or_last_value();
void nth_value_function();
void nth_row();
void from_first_or_last();
void window_name_or_specification();
void in_line_window_specification();
void case_expression();
void case_abbreviation();
void case_specification();
void simple_case();
void searched_case();
void simple_when_clause();
void searched_when_clause();
void else_clause();
void case_operand();
void when_operand_list();
void when_operand();
void result();
void cast_specification();
void cast_operand();
void cast_target();
void next_value_expression();
void field_reference();
void subtype_treatment();
void target_subtype();
void method_invocation();
void direct_invocation();
void generalized_invocation();
void static_method_invocation();
void new_specification();
void new_invocation();
void attribute_or_method_reference();
void dereference_operation();
void reference_resolution();
void array_element_reference();
void multiset_element_reference();
void value_expression();
void common_value_expression();
void user_defined_type_value_expression();
void reference_value_expression();
void collection_value_expression();
void numeric_value_expression();
void term();
void factor();
void numeric_primary();
void numeric_value_function();
void position_expression();
void regex_occurrences_function();
void regex_position_expression();
void regex_position_start_or_after();
void character_position_expression();
void binary_position_expression();
void length_expression();
void char_length_expression();
void octet_length_expression();
void extract_expression();
void extract_field();
void time_zone_field();
void extract_source();
void cardinality_expression();
void max_cardinality_expression();
void absolute_value_expression();
void modulus_expression();
void natural_logarithm();
void exponential_function();
void power_function();
void square_root();
void floor_function();
void ceiling_function();
void width_bucket_function();
void string_value_expression();
void character_value_expression();
void concatenation();
void character_factor();
void character_primary();
void binary_value_expression();
void binary_primary();
void binary_concatenation();
void string_value_function();
void character_value_function();
void character_substring_function();
void regular_expression_substring_function();
void regex_substring_function();
void fold();
void transcoding();
void character_transliteration();
void regex_transliteration();
void regex_transliteration_occurrence();
void trim_function();
void trim_operands();
void trim_specification();
void character_overlay_function();
void normalize_function();
void normal_form();
void normalize_function_result_length();
void specific_type_method();
void binary_value_function();
void binary_substring_function();
void binary_trim_function();
void binary_trim_operands();
void binary_overlay_function();
void datetime_value_expression();
void datetime_term();
void datetime_factor();
void datetime_primary();
void time_zone();
void time_zone_specifier();
void datetime_value_function();
void current_date_value_function();
void current_time_value_function();
void current_local_time_value_function();
void current_timestamp_value_function();
void current_local_timestamp_value_function();
void interval_value_expression();
void interval_term();
void interval_factor();
void interval_primary();
void interval_value_function();
void interval_absolute_value_function();
void boolean_value_expression();
void boolean_term();
void boolean_factor();
void boolean_test();
void truth_value();
void boolean_primary();
void boolean_predicand();
void parenthesized_boolean_value_expression();
void array_value_expression();
void array_value_expression_1();
void array_primary();
void array_value_function();
void trim_array_function();
void array_value_constructor();
void array_value_constructor_by_enumeration();
void array_element_list();
void array_element();
void array_value_constructor_by_query();
void multiset_value_expression();
void multiset_term();
void multiset_primary();
void multiset_set_function();
void multiset_value_constructor();
void multiset_value_constructor_by_enumeration();
void multiset_element_list();
void multiset_element();
void multiset_value_constructor_by_query();
void table_value_constructor_by_query();
void row_value_constructor();
void explicit_row_value_constructor();
void row_value_constructor_element_list();
void row_value_constructor_element();
void contextually_typed_row_value_constructor();
void contextually_typed_row_value_constructor_element_list();
void contextually_typed_row_value_constructor_element();
void row_value_constructor_predicand();
void row_value_expression();
void table_row_value_expression();
void contextually_typed_row_value_expression();
void row_value_predicand();
void row_value_special_case();
void table_value_constructor();
void row_value_expression_list();
void contextually_typed_table_value_constructor();
void contextually_typed_row_value_expression_list();
void table_expression();
void from_clause();
void table_reference_list();
void table_reference();
void table_factor();
void sample_clause();
void sample_method();
void repeatable_clause();
void sample_percentage();
void repeat_argument();
void table_primary();
void alias();
void system_version_specification();
void only_spec();
void lateral_derived_table();
void collection_derived_table();
void table_function_derived_table();
void derived_table();
void table_or_query_name();
void column_name_list();
void data_change_delta_table();
void data_change_statement();
void result_option();
void parenthesized_joined_table();
void joined_table();
void cross_join();
void qualified_join();
void partitioned_join_table();
void partitioned_join_column_reference_list();
void partitioned_join_column_reference();
void natural_join();
void join_specification();
void join_condition();
void named_columns_join();
void join_type();
void outer_join_type();
void join_column_list();
void where_clause();
void group_by_clause();
void grouping_element_list();
void grouping_element();
void ordinary_grouping_set();
void grouping_column_reference();
void grouping_column_reference_list();
void rollup_list();
void ordinary_grouping_set_list();
void cube_list();
void grouping_sets_specification();
void grouping_set_list();
void grouping_set();
void empty_grouping_set();
void having_clause();
void window_clause();
void window_definition_list();
void window_definition();
void window_specification();
void window_specification_details();
void existing_identifier();
void window_partition_clause();
void window_partition_column_reference_list();
void window_partition_column_reference();
void window_order_clause();
void window_frame_clause();
void window_frame_units();
void window_frame_extent();
void window_frame_start();
void window_frame_preceding();
void window_frame_between();
void window_frame_bound();
void window_frame_following();
void window_frame_exclusion();
void query_specification();
void select_list();
void star();
void select_sublist();
void qualified_asterisk();
void asterisked_identifier_chain();
void derived_column();
void as_clause();
void all_fields_reference();
void all_fields_column_name_list();
void query_expression();
void with_clause();
void with_list();
void with_list_element();
void query_expression_body();
void query_term();
void query_primary();
void simple_table();
void explicit_table();
void corresponding_spec();
void order_by_clause();
void result_offset_clause();
void fetch_first_clause();
void search_or_cycle_clause();
void search_clause();
void recursive_search_order();
void cycle_clause();
void cycle_column_list();
void subquery();
void predicate();
void comparison_predicate();
void comparison_predicate_part_2();
void comp_op();
void between_predicate();
void between_predicate_part_2();
void in_predicate();
void in_predicate_part_2();
void in_predicate_value();
void in_value_list();
void like_predicate();
void character_like_predicate();
void character_like_predicate_part_2();
void octet_like_predicate();
void octet_like_predicate_part_2();
void similar_predicate();
void similar_predicate_part_2();
void regex_like_predicate();
void regex_like_predicate_part_2();
void null_predicate();
void null_predicate_part_2();
void quantified_comparison_predicate();
void quantified_comparison_predicate_part_2();
void exists_predicate();
void unique_predicate();
void normalized_predicate();
void normalized_predicate_part_2();
void match_predicate();
void match_predicate_part_2();
void overlaps_predicate();
void overlaps_predicate_part_1();
void overlaps_predicate_part_2();
void row_value_predicand_1();
void row_value_predicand_2();
void distinct_predicate();
void distinct_predicate_part_2();
void row_value_predicand_3();
void row_value_predicand_4();
void member_predicate();
void member_predicate_part_2();
void submultiset_predicate();
void submultiset_predicate_part_2();
void set_predicate();
void set_predicate_part_2();
void type_predicate();
void type_predicate_part_2();
void type_list();
void user_defined_type_specification();
void inclusive_user_defined_type_specification();
void exclusive_user_defined_type_specification();
void search_condition();
void interval_qualifier();
void start_field();
void end_field();
void single_datetime_field();
void primary_datetime_field();
void non_second_primary_datetime_field();
void interval_fractional_seconds_precision();
void interval_leading_field_precision();
void language_clause();
void language_name();
void path_specification();
void schema_name_list();
void routine_invocation();
void routine_name();
void SQL_argument_list();
void SQL_argument();
void generalized_expression();
void named_argument_specification();
void named_argument_SQL_argument();
void character_set_specification();
void standard_character_set_name();
void implementation_defined_character_set_name();
void user_defined_character_set_name();
void specific_routine_designator();
void routine_type();
void member_name();
void member_name_alternatives();
void data_type_list();
void collate_clause();
void constraint_name_definition();
void constraint_characteristics();
void constraint_check_time();
void constraint_enforcement();
void aggregate_function();
void general_set_function();
void set_function_type();
void computational_operation();
void set_quantifier();
void filter_clause();
void binary_set_function();
void binary_set_function_type();
void dependent_variable_expression();
void independent_variable_expression();
void ordered_set_function();
void hypothetical_set_function();
void within_group_specification();
void hypothetical_set_function_value_expression_list();
void inverse_distribution_function();
void inverse_distribution_function_argument();
void inverse_distribution_function_type();
void array_aggregate_function();
void sort_specification_list();
void sort_specification();
void sort_key();
void ordering_specification();
void null_ordering();
void schema_definition();
void schema_character_set_or_path();
void schema_name_clause();
void schema_character_set_specification();
void schema_path_specification();
void schema_element();
void drop_schema_statement();
void drop_behavior();
void table_definition();
void table_contents_source();
void table_scope();
void global_or_local();
void system_versioning_clause();
void retention_period_specification();
void length_of_time();
void time_unit();
void table_commit_action();
void table_element_list();
void table_element();
void typed_table_clause();
void typed_table_element_list();
void typed_table_element();
void self_referencing_column_specification();
void reference_generation();
void column_options();
void column_option_list();
void subtable_clause();
void supertable_clause();
void supertable_name();
void like_clause();
void like_options();
void like_option();
void identity_option();
void column_default_option();
void generation_option();
void as_subquery_clause();
void with_or_without_data();
void column_definition();
void data_type_or_schema_qualified_name();
void system_version_start_column_specification();
void system_version_end_column_specification();
void timestamp_generation_rule();
void column_constraint_definition();
void column_constraint();
void identity_column_specification();
void generation_clause();
void generation_rule();
void generation_expression();
void default_clause();
void default_option();
void table_constraint_definition();
void table_constraint();
void unique_constraint_definition();
void unique_specification();
void unique_column_list();
void referential_constraint_definition();
void references_specification();
void match_type();
void referencing_columns();
void referenced_table_and_columns();
void reference_column_list();
void referential_triggered_action();
void update_rule();
void delete_rule();
void referential_action();
void check_constraint_definition();
void alter_table_statement();
void alter_table_action();
void add_column_definition();
void alter_column_definition();
void alter_column_action();
void set_column_default_clause();
void drop_column_default_clause();
void set_column_not_null_clause();
void drop_column_not_null_clause();
void add_column_scope_clause();
void drop_column_scope_clause();
void alter_column_data_type_clause();
void alter_identity_column_specification();
void set_identity_column_generation_clause();
void alter_identity_column_option();
void drop_identity_property_clause();
void drop_column_generation_expression_clause();
void drop_column_definition();
void add_table_constraint_definition();
void alter_table_constraint_definition();
void drop_table_constraint_definition();
void add_system_versioning_clause();
void add_system_version_column_list();
void column_definition_1();
void column_definition_2();
void alter_system_versioning_clause();
void drop_system_versioning_clause();
void drop_table_statement();
void view_definition();
void view_specification();
void regular_view_specification();
void referenceable_view_specification();
void subview_clause();
void view_element_list();
void view_element();
void view_column_option();
void levels_clause();
void view_column_list();
void drop_view_statement();
void domain_definition();
void domain_constraint();
void alter_domain_statement();
void alter_domain_action();
void set_domain_default_clause();
void drop_domain_default_clause();
void add_domain_constraint_definition();
void drop_domain_constraint_definition();
void drop_domain_statement();
void character_set_definition();
void character_set_source();
void drop_character_set_statement();
void collation_definition();
void pad_characteristic();
void drop_collation_statement();
void transliteration_definition();
void source_character_set_specification();
void target_character_set_specification();
void transliteration_source();
void transliteration_routine();
void drop_transliteration_statement();
void assertion_definition();
void drop_assertion_statement();
void trigger_definition();
void trigger_action_time();
void trigger_event();
void trigger_column_list();
void triggered_action();
void triggered_when_clause();
void triggered_SQL_statement();
void transition_table_or_variable_list();
void transition_table_or_variable();
void drop_trigger_statement();
void user_defined_type_definition();
void user_defined_type_body();
void user_defined_type_option_list();
void user_defined_type_option();
void subtype_clause();
void supertype_name();
void representation();
void member_list();
void member();
void instantiable_clause();
void finality();
void reference_type_specification();
void user_defined_representation();
void derived_representation();
void system_generated_representation();
void cast_to_ref();
void cast_to_type();
void list_of_attributes();
void cast_to_distinct();
void cast_to_source();
void method_specification_list();
void method_specification();
void original_method_specification();
void overriding_method_specification();
void partial_method_specification();
void specific_identifier();
void method_characteristics();
void method_characteristic();
void attribute_definition();
void attribute_default();
void alter_type_statement();
void alter_type_action();
void add_attribute_definition();
void drop_attribute_definition();
void add_original_method_specification();
void add_overriding_method_specification();
void drop_method_specification();
void specific_method_specification_designator();
void drop_data_type_statement();
void SQL_invoked_routine();
void schema_routine();
void schema_procedure();
void schema_function();
void SQL_invoked_procedure();
void SQL_invoked_function();
void SQL_parameter_declaration_list();
void SQL_parameter_declaration();
void parameter_default();
void parameter_mode();
void parameter_type();
void locator_indication();
void function_specification();
void method_specification_designator();
void routine_characteristics();
void routine_characteristic();
void savepoint_level_indication();
void returned_result_sets_characteristic();
void parameter_style_clause();
void dispatch_clause();
void returns_clause();
void returns_type();
void returns_table_type();
void table_function_column_list();
void table_function_column_list_element();
void result_cast();
void result_cast_from_type();
void returns_data_type();
void routine_body();
void SQL_routine_spec();
void rights_clause();
void SQL_routine_body();
void external_body_reference();
void external_security_clause();
void parameter_style();
void deterministic_characteristic();
void SQL_data_access_indication();
void null_call_clause();
void maximum_returned_result_sets();
void transform_group_specification();
void single_group_specification();
void multiple_group_specification();
void group_specification();
void alter_routine_statement();
void alter_routine_characteristics();
void alter_routine_characteristic();
void alter_routine_behavior();
void drop_routine_statement();
void user_defined_cast_definition();
void cast_function();
void source_data_type();
void target_data_type();
void drop_user_defined_cast_statement();
void user_defined_ordering_definition();
void ordering_form();
void equals_ordering_form();
void full_ordering_form();
void ordering_category();
void relative_category();
void map_category();
void state_category();
void relative_function_specification();
void map_function_specification();
void drop_user_defined_ordering_statement();
void transform_definition();
void transform_group();
void transform_element_list();
void transform_element();
void to_sql();
void from_sql();
void to_sql_function();
void from_sql_function();
void alter_transform_statement();
void alter_group();
void alter_transform_action_list();
void alter_transform_action();
void add_transform_element_list();
void drop_transform_element_list();
void transform_kind();
void drop_transform_statement();
void transforms_to_be_dropped();
void transform_group_element();
void sequence_generator_definition();
void sequence_generator_options();
void sequence_generator_option();
void common_sequence_generator_options();
void common_sequence_generator_option();
void basic_sequence_generator_option();
void sequence_generator_data_type_option();
void sequence_generator_start_with_option();
void sequence_generator_start_value();
void sequence_generator_increment_by_option();
void sequence_generator_increment();
void sequence_generator_maxvalue_option();
void sequence_generator_max_value();
void sequence_generator_minvalue_option();
void sequence_generator_min_value();
void sequence_generator_cycle_option();
void alter_sequence_generator_statement();
void alter_sequence_generator_options();
void alter_sequence_generator_option();
void alter_sequence_generator_restart_option();
void sequence_generator_restart_value();
void drop_sequence_generator_statement();
void grant_statement();
void grant_privilege_statement();
void privileges();
void object_name();
void object_privileges();
void action();
void privilege_method_list();
void privilege_column_list();
void grantee();
void grantor();
void role_definition();
void grant_role_statement();
void drop_role_statement();
void revoke_statement();
void revoke_privilege_statement();
void revoke_option_extension();
void revoke_role_statement();
void SQL_client_module_definition();
void module_authorization_clause();
void module_path_specification();
void module_transform_group_specification();
void module_collations();
void module_collation_specification();
void character_set_specification_list();
void module_contents();
void module_name_clause();
void module_character_set_specification();
void externally_invoked_procedure();
void host_parameter_declaration_list();
void host_parameter_declaration();
void host_parameter_data_type();
void status_parameter();
void SQL_procedure_statement();
void SQL_executable_statement();
void SQL_schema_statement();
void SQL_schema_definition_statement();
void SQL_schema_manipulation_statement();
void SQL_data_statement();
void SQL_data_change_statement();
void SQL_control_statement();
void SQL_transaction_statement();
void SQL_connection_statement();
void SQL_session_statement();
void SQL_diagnostics_statement();
void SQL_dynamic_statement();
void SQL_dynamic_data_statement();
void SQL_descriptor_statement();
void declare_cursor();
void cursor_properties();
void cursor_sensitivity();
void cursor_scrollability();
void cursor_holdability();
void cursor_returnability();
void cursor_specification();
void updatability_clause();
void open_statement();
void fetch_statement();
void fetch_orientation();
void fetch_target_list();
void close_statement();
void select_statement_single_row();
void select_target_list();
void delete_statement_positioned();
void target_table();
void delete_statement_searched();
void truncate_table_statement();
void identity_column_restart_option();
void insert_statement();
void insertion_target();
void insert_columns_and_source();
void from_subquery();
void from_constructor();
void override_clause();
void from_default();
void insert_column_list();
void merge_statement();
void merge_operation_specification();
void merge_when_clause();
void merge_when_matched_clause();
void merge_update_or_delete_specification();
void merge_when_not_matched_clause();
void merge_update_specification();
void merge_delete_specification();
void merge_insert_specification();
void merge_insert_value_list();
void merge_insert_value_element();
void update_statement_positioned();
void update_statement_searched();
void set_clause_list();
void set_clause();
void set_target();
void multiple_column_assignment();
void set_target_list();
void assigned_row();
void update_target();
void mutated_set_clause();
void mutated_target();
void update_source();
void temporary_table_declaration();
void call_statement();
void return_statement();
void return_value();
void start_transaction_statement();
void set_transaction_statement();
void transaction_characteristics();
void transaction_mode();
void transaction_access_mode();
void isolation_level();
void level_of_isolation();
void diagnostics_size();
void set_constraints_mode_statement();
void constraint_name_list();
void savepoint_statement();
void savepoint_specifier();
void release_savepoint_statement();
void commit_statement();
void rollback_statement();
void savepoint_clause();
void connect_statement();
void connection_target();
void set_connection_statement();
void connection_object();
void disconnect_statement();
void disconnect_object();
void set_session_characteristics_statement();
void session_characteristic_list();
void session_characteristic();
void session_transaction_characteristics();
void set_session_user_identifier_statement();
void set_role_statement();
void role_specification();
void set_local_time_zone_statement();
void set_time_zone_value();
void set_catalog_statement();
void catalog_name_characteristic();
void set_schema_statement();
void schema_name_characteristic();
void set_names_statement();
void character_set_name_characteristic();
void set_path_statement();
void SQL_path_characteristic();
void set_transform_group_statement();
void transform_group_characteristic();
void set_session_collation_statement();
void collation_specification();
void allocate_descriptor_statement();
void deallocate_descriptor_statement();
void get_descriptor_statement();
void get_descriptor_information();
void get_header_information();
void header_item_name();
void get_item_information();
void simple_target_specification_1();
void simple_target_specification_2();
void descriptor_item_name();
void set_descriptor_statement();
void set_descriptor_information();
void set_header_information();
void set_item_information();
void prepare_statement();
void attributes_specification();
void preparable_statement();
void preparable_SQL_data_statement();
void preparable_SQL_schema_statement();
void preparable_SQL_transaction_statement();
void preparable_SQL_control_statement();
void preparable_SQL_session_statement();
void dynamic_select_statement();
void preparable_implementation_defined_statement();
void cursor_attributes();
void cursor_attribute();
void deallocate_prepared_statement();
void describe_statement();
void describe_input_statement();
void describe_output_statement();
void nesting_option();
void using_descriptor();
void described_object();
void input_using_clause();
void using_arguments();
void using_argument();
void using_input_descriptor();
void output_using_clause();
void into_arguments();
void into_argument();
void into_descriptor();
void execute_statement();
void result_using_clause();
void parameter_using_clause();
void execute_immediate_statement();
void dynamic_declare_cursor();
void allocate_cursor_statement();
void cursor_intent();
void statement_cursor();
void result_set_cursor();
void dynamic_open_statement();
void dynamic_fetch_statement();
void dynamic_single_row_select_statement();
void dynamic_close_statement();
void dynamic_delete_statement_positioned();
void dynamic_update_statement_positioned();
void preparable_dynamic_delete_statement_positioned();
void preparable_dynamic_cursor_name();
void preparable_dynamic_update_statement_positioned();
void direct_SQL_statement();
void directly_executable_statement();
void direct_SQL_data_statement();
void direct_implementation_defined_statement();
void direct_select_statement_multiple_rows();
void get_diagnostics_statement();
void SQL_diagnostics_information();
void statement_information();
void statement_information_item();
void statement_information_item_name();
void condition_information();
void condition_information_item();
void condition_information_item_name();
void all_information();
void all_info_target();
void all_qualifier();
void use_statement();
void lambda();
void lambda_body();
void lambda_params();
void if_not_exists();
void identifier_suffix_chain();
void limit_clause();
void presto_generic_type();
void presto_array_type();
void presto_map_type();
void percent_operator();
void distinct();
void grouping_expression();
void count();
void table_description();
void routine_description();
void column_description();
void presto_aggregation_function();
void presto_aggregations();
void try_cast();
void varbinary();
void table_attributes();
void or_replace();
void udaf_filter();
void extra_args_to_agg();
void weird_identifiers();
 inline bool jj_2_1(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1() || jj_done);
  }

 inline bool jj_2_2(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_2() || jj_done);
  }

 inline bool jj_2_3(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_3() || jj_done);
  }

 inline bool jj_2_4(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_4() || jj_done);
  }

 inline bool jj_2_5(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_5() || jj_done);
  }

 inline bool jj_2_6(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_6() || jj_done);
  }

 inline bool jj_2_7(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_7() || jj_done);
  }

 inline bool jj_2_8(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_8() || jj_done);
  }

 inline bool jj_2_9(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_9() || jj_done);
  }

 inline bool jj_2_10(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_10() || jj_done);
  }

 inline bool jj_2_11(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_11() || jj_done);
  }

 inline bool jj_2_12(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_12() || jj_done);
  }

 inline bool jj_2_13(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_13() || jj_done);
  }

 inline bool jj_2_14(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_14() || jj_done);
  }

 inline bool jj_2_15(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_15() || jj_done);
  }

 inline bool jj_2_16(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_16() || jj_done);
  }

 inline bool jj_2_17(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_17() || jj_done);
  }

 inline bool jj_2_18(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_18() || jj_done);
  }

 inline bool jj_2_19(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_19() || jj_done);
  }

 inline bool jj_2_20(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_20() || jj_done);
  }

 inline bool jj_2_21(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_21() || jj_done);
  }

 inline bool jj_2_22(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_22() || jj_done);
  }

 inline bool jj_2_23(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_23() || jj_done);
  }

 inline bool jj_2_24(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_24() || jj_done);
  }

 inline bool jj_2_25(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_25() || jj_done);
  }

 inline bool jj_2_26(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_26() || jj_done);
  }

 inline bool jj_2_27(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_27() || jj_done);
  }

 inline bool jj_2_28(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_28() || jj_done);
  }

 inline bool jj_2_29(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_29() || jj_done);
  }

 inline bool jj_2_30(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_30() || jj_done);
  }

 inline bool jj_2_31(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_31() || jj_done);
  }

 inline bool jj_2_32(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_32() || jj_done);
  }

 inline bool jj_2_33(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_33() || jj_done);
  }

 inline bool jj_2_34(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_34() || jj_done);
  }

 inline bool jj_2_35(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_35() || jj_done);
  }

 inline bool jj_2_36(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_36() || jj_done);
  }

 inline bool jj_2_37(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_37() || jj_done);
  }

 inline bool jj_2_38(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_38() || jj_done);
  }

 inline bool jj_2_39(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_39() || jj_done);
  }

 inline bool jj_2_40(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_40() || jj_done);
  }

 inline bool jj_2_41(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_41() || jj_done);
  }

 inline bool jj_2_42(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_42() || jj_done);
  }

 inline bool jj_2_43(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_43() || jj_done);
  }

 inline bool jj_2_44(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_44() || jj_done);
  }

 inline bool jj_2_45(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_45() || jj_done);
  }

 inline bool jj_2_46(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_46() || jj_done);
  }

 inline bool jj_2_47(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_47() || jj_done);
  }

 inline bool jj_2_48(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_48() || jj_done);
  }

 inline bool jj_2_49(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_49() || jj_done);
  }

 inline bool jj_2_50(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_50() || jj_done);
  }

 inline bool jj_2_51(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_51() || jj_done);
  }

 inline bool jj_2_52(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_52() || jj_done);
  }

 inline bool jj_2_53(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_53() || jj_done);
  }

 inline bool jj_2_54(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_54() || jj_done);
  }

 inline bool jj_2_55(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_55() || jj_done);
  }

 inline bool jj_2_56(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_56() || jj_done);
  }

 inline bool jj_2_57(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_57() || jj_done);
  }

 inline bool jj_2_58(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_58() || jj_done);
  }

 inline bool jj_2_59(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_59() || jj_done);
  }

 inline bool jj_2_60(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_60() || jj_done);
  }

 inline bool jj_2_61(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_61() || jj_done);
  }

 inline bool jj_2_62(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_62() || jj_done);
  }

 inline bool jj_2_63(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_63() || jj_done);
  }

 inline bool jj_2_64(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_64() || jj_done);
  }

 inline bool jj_2_65(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_65() || jj_done);
  }

 inline bool jj_2_66(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_66() || jj_done);
  }

 inline bool jj_2_67(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_67() || jj_done);
  }

 inline bool jj_2_68(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_68() || jj_done);
  }

 inline bool jj_2_69(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_69() || jj_done);
  }

 inline bool jj_2_70(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_70() || jj_done);
  }

 inline bool jj_2_71(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_71() || jj_done);
  }

 inline bool jj_2_72(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_72() || jj_done);
  }

 inline bool jj_2_73(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_73() || jj_done);
  }

 inline bool jj_2_74(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_74() || jj_done);
  }

 inline bool jj_2_75(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_75() || jj_done);
  }

 inline bool jj_2_76(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_76() || jj_done);
  }

 inline bool jj_2_77(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_77() || jj_done);
  }

 inline bool jj_2_78(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_78() || jj_done);
  }

 inline bool jj_2_79(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_79() || jj_done);
  }

 inline bool jj_2_80(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_80() || jj_done);
  }

 inline bool jj_2_81(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_81() || jj_done);
  }

 inline bool jj_2_82(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_82() || jj_done);
  }

 inline bool jj_2_83(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_83() || jj_done);
  }

 inline bool jj_2_84(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_84() || jj_done);
  }

 inline bool jj_2_85(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_85() || jj_done);
  }

 inline bool jj_2_86(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_86() || jj_done);
  }

 inline bool jj_2_87(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_87() || jj_done);
  }

 inline bool jj_2_88(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_88() || jj_done);
  }

 inline bool jj_2_89(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_89() || jj_done);
  }

 inline bool jj_2_90(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_90() || jj_done);
  }

 inline bool jj_2_91(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_91() || jj_done);
  }

 inline bool jj_2_92(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_92() || jj_done);
  }

 inline bool jj_2_93(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_93() || jj_done);
  }

 inline bool jj_2_94(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_94() || jj_done);
  }

 inline bool jj_2_95(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_95() || jj_done);
  }

 inline bool jj_2_96(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_96() || jj_done);
  }

 inline bool jj_2_97(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_97() || jj_done);
  }

 inline bool jj_2_98(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_98() || jj_done);
  }

 inline bool jj_2_99(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_99() || jj_done);
  }

 inline bool jj_2_100(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_100() || jj_done);
  }

 inline bool jj_2_101(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_101() || jj_done);
  }

 inline bool jj_2_102(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_102() || jj_done);
  }

 inline bool jj_2_103(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_103() || jj_done);
  }

 inline bool jj_2_104(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_104() || jj_done);
  }

 inline bool jj_2_105(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_105() || jj_done);
  }

 inline bool jj_2_106(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_106() || jj_done);
  }

 inline bool jj_2_107(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_107() || jj_done);
  }

 inline bool jj_2_108(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_108() || jj_done);
  }

 inline bool jj_2_109(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_109() || jj_done);
  }

 inline bool jj_2_110(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_110() || jj_done);
  }

 inline bool jj_2_111(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_111() || jj_done);
  }

 inline bool jj_2_112(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_112() || jj_done);
  }

 inline bool jj_2_113(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_113() || jj_done);
  }

 inline bool jj_2_114(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_114() || jj_done);
  }

 inline bool jj_2_115(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_115() || jj_done);
  }

 inline bool jj_2_116(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_116() || jj_done);
  }

 inline bool jj_2_117(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_117() || jj_done);
  }

 inline bool jj_2_118(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_118() || jj_done);
  }

 inline bool jj_2_119(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_119() || jj_done);
  }

 inline bool jj_2_120(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_120() || jj_done);
  }

 inline bool jj_2_121(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_121() || jj_done);
  }

 inline bool jj_2_122(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_122() || jj_done);
  }

 inline bool jj_2_123(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_123() || jj_done);
  }

 inline bool jj_2_124(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_124() || jj_done);
  }

 inline bool jj_2_125(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_125() || jj_done);
  }

 inline bool jj_2_126(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_126() || jj_done);
  }

 inline bool jj_2_127(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_127() || jj_done);
  }

 inline bool jj_2_128(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_128() || jj_done);
  }

 inline bool jj_2_129(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_129() || jj_done);
  }

 inline bool jj_2_130(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_130() || jj_done);
  }

 inline bool jj_2_131(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_131() || jj_done);
  }

 inline bool jj_2_132(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_132() || jj_done);
  }

 inline bool jj_2_133(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_133() || jj_done);
  }

 inline bool jj_2_134(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_134() || jj_done);
  }

 inline bool jj_2_135(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_135() || jj_done);
  }

 inline bool jj_2_136(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_136() || jj_done);
  }

 inline bool jj_2_137(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_137() || jj_done);
  }

 inline bool jj_2_138(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_138() || jj_done);
  }

 inline bool jj_2_139(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_139() || jj_done);
  }

 inline bool jj_2_140(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_140() || jj_done);
  }

 inline bool jj_2_141(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_141() || jj_done);
  }

 inline bool jj_2_142(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_142() || jj_done);
  }

 inline bool jj_2_143(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_143() || jj_done);
  }

 inline bool jj_2_144(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_144() || jj_done);
  }

 inline bool jj_2_145(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_145() || jj_done);
  }

 inline bool jj_2_146(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_146() || jj_done);
  }

 inline bool jj_2_147(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_147() || jj_done);
  }

 inline bool jj_2_148(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_148() || jj_done);
  }

 inline bool jj_2_149(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_149() || jj_done);
  }

 inline bool jj_2_150(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_150() || jj_done);
  }

 inline bool jj_2_151(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_151() || jj_done);
  }

 inline bool jj_2_152(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_152() || jj_done);
  }

 inline bool jj_2_153(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_153() || jj_done);
  }

 inline bool jj_2_154(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_154() || jj_done);
  }

 inline bool jj_2_155(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_155() || jj_done);
  }

 inline bool jj_2_156(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_156() || jj_done);
  }

 inline bool jj_2_157(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_157() || jj_done);
  }

 inline bool jj_2_158(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_158() || jj_done);
  }

 inline bool jj_2_159(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_159() || jj_done);
  }

 inline bool jj_2_160(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_160() || jj_done);
  }

 inline bool jj_2_161(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_161() || jj_done);
  }

 inline bool jj_2_162(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_162() || jj_done);
  }

 inline bool jj_2_163(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_163() || jj_done);
  }

 inline bool jj_2_164(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_164() || jj_done);
  }

 inline bool jj_2_165(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_165() || jj_done);
  }

 inline bool jj_2_166(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_166() || jj_done);
  }

 inline bool jj_2_167(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_167() || jj_done);
  }

 inline bool jj_2_168(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_168() || jj_done);
  }

 inline bool jj_2_169(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_169() || jj_done);
  }

 inline bool jj_2_170(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_170() || jj_done);
  }

 inline bool jj_2_171(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_171() || jj_done);
  }

 inline bool jj_2_172(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_172() || jj_done);
  }

 inline bool jj_2_173(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_173() || jj_done);
  }

 inline bool jj_2_174(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_174() || jj_done);
  }

 inline bool jj_2_175(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_175() || jj_done);
  }

 inline bool jj_2_176(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_176() || jj_done);
  }

 inline bool jj_2_177(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_177() || jj_done);
  }

 inline bool jj_2_178(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_178() || jj_done);
  }

 inline bool jj_2_179(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_179() || jj_done);
  }

 inline bool jj_2_180(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_180() || jj_done);
  }

 inline bool jj_2_181(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_181() || jj_done);
  }

 inline bool jj_2_182(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_182() || jj_done);
  }

 inline bool jj_2_183(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_183() || jj_done);
  }

 inline bool jj_2_184(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_184() || jj_done);
  }

 inline bool jj_2_185(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_185() || jj_done);
  }

 inline bool jj_2_186(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_186() || jj_done);
  }

 inline bool jj_2_187(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_187() || jj_done);
  }

 inline bool jj_2_188(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_188() || jj_done);
  }

 inline bool jj_2_189(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_189() || jj_done);
  }

 inline bool jj_2_190(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_190() || jj_done);
  }

 inline bool jj_2_191(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_191() || jj_done);
  }

 inline bool jj_2_192(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_192() || jj_done);
  }

 inline bool jj_2_193(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_193() || jj_done);
  }

 inline bool jj_2_194(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_194() || jj_done);
  }

 inline bool jj_2_195(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_195() || jj_done);
  }

 inline bool jj_2_196(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_196() || jj_done);
  }

 inline bool jj_2_197(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_197() || jj_done);
  }

 inline bool jj_2_198(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_198() || jj_done);
  }

 inline bool jj_2_199(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_199() || jj_done);
  }

 inline bool jj_2_200(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_200() || jj_done);
  }

 inline bool jj_2_201(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_201() || jj_done);
  }

 inline bool jj_2_202(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_202() || jj_done);
  }

 inline bool jj_2_203(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_203() || jj_done);
  }

 inline bool jj_2_204(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_204() || jj_done);
  }

 inline bool jj_2_205(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_205() || jj_done);
  }

 inline bool jj_2_206(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_206() || jj_done);
  }

 inline bool jj_2_207(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_207() || jj_done);
  }

 inline bool jj_2_208(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_208() || jj_done);
  }

 inline bool jj_2_209(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_209() || jj_done);
  }

 inline bool jj_2_210(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_210() || jj_done);
  }

 inline bool jj_2_211(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_211() || jj_done);
  }

 inline bool jj_2_212(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_212() || jj_done);
  }

 inline bool jj_2_213(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_213() || jj_done);
  }

 inline bool jj_2_214(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_214() || jj_done);
  }

 inline bool jj_2_215(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_215() || jj_done);
  }

 inline bool jj_2_216(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_216() || jj_done);
  }

 inline bool jj_2_217(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_217() || jj_done);
  }

 inline bool jj_2_218(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_218() || jj_done);
  }

 inline bool jj_2_219(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_219() || jj_done);
  }

 inline bool jj_2_220(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_220() || jj_done);
  }

 inline bool jj_2_221(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_221() || jj_done);
  }

 inline bool jj_2_222(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_222() || jj_done);
  }

 inline bool jj_2_223(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_223() || jj_done);
  }

 inline bool jj_2_224(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_224() || jj_done);
  }

 inline bool jj_2_225(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_225() || jj_done);
  }

 inline bool jj_2_226(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_226() || jj_done);
  }

 inline bool jj_2_227(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_227() || jj_done);
  }

 inline bool jj_2_228(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_228() || jj_done);
  }

 inline bool jj_2_229(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_229() || jj_done);
  }

 inline bool jj_2_230(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_230() || jj_done);
  }

 inline bool jj_2_231(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_231() || jj_done);
  }

 inline bool jj_2_232(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_232() || jj_done);
  }

 inline bool jj_2_233(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_233() || jj_done);
  }

 inline bool jj_2_234(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_234() || jj_done);
  }

 inline bool jj_2_235(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_235() || jj_done);
  }

 inline bool jj_2_236(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_236() || jj_done);
  }

 inline bool jj_2_237(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_237() || jj_done);
  }

 inline bool jj_2_238(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_238() || jj_done);
  }

 inline bool jj_2_239(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_239() || jj_done);
  }

 inline bool jj_2_240(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_240() || jj_done);
  }

 inline bool jj_2_241(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_241() || jj_done);
  }

 inline bool jj_2_242(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_242() || jj_done);
  }

 inline bool jj_2_243(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_243() || jj_done);
  }

 inline bool jj_2_244(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_244() || jj_done);
  }

 inline bool jj_2_245(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_245() || jj_done);
  }

 inline bool jj_2_246(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_246() || jj_done);
  }

 inline bool jj_2_247(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_247() || jj_done);
  }

 inline bool jj_2_248(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_248() || jj_done);
  }

 inline bool jj_2_249(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_249() || jj_done);
  }

 inline bool jj_2_250(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_250() || jj_done);
  }

 inline bool jj_2_251(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_251() || jj_done);
  }

 inline bool jj_2_252(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_252() || jj_done);
  }

 inline bool jj_2_253(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_253() || jj_done);
  }

 inline bool jj_2_254(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_254() || jj_done);
  }

 inline bool jj_2_255(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_255() || jj_done);
  }

 inline bool jj_2_256(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_256() || jj_done);
  }

 inline bool jj_2_257(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_257() || jj_done);
  }

 inline bool jj_2_258(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_258() || jj_done);
  }

 inline bool jj_2_259(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_259() || jj_done);
  }

 inline bool jj_2_260(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_260() || jj_done);
  }

 inline bool jj_2_261(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_261() || jj_done);
  }

 inline bool jj_2_262(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_262() || jj_done);
  }

 inline bool jj_2_263(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_263() || jj_done);
  }

 inline bool jj_2_264(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_264() || jj_done);
  }

 inline bool jj_2_265(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_265() || jj_done);
  }

 inline bool jj_2_266(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_266() || jj_done);
  }

 inline bool jj_2_267(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_267() || jj_done);
  }

 inline bool jj_2_268(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_268() || jj_done);
  }

 inline bool jj_2_269(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_269() || jj_done);
  }

 inline bool jj_2_270(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_270() || jj_done);
  }

 inline bool jj_2_271(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_271() || jj_done);
  }

 inline bool jj_2_272(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_272() || jj_done);
  }

 inline bool jj_2_273(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_273() || jj_done);
  }

 inline bool jj_2_274(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_274() || jj_done);
  }

 inline bool jj_2_275(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_275() || jj_done);
  }

 inline bool jj_2_276(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_276() || jj_done);
  }

 inline bool jj_2_277(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_277() || jj_done);
  }

 inline bool jj_2_278(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_278() || jj_done);
  }

 inline bool jj_2_279(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_279() || jj_done);
  }

 inline bool jj_2_280(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_280() || jj_done);
  }

 inline bool jj_2_281(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_281() || jj_done);
  }

 inline bool jj_2_282(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_282() || jj_done);
  }

 inline bool jj_2_283(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_283() || jj_done);
  }

 inline bool jj_2_284(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_284() || jj_done);
  }

 inline bool jj_2_285(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_285() || jj_done);
  }

 inline bool jj_2_286(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_286() || jj_done);
  }

 inline bool jj_2_287(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_287() || jj_done);
  }

 inline bool jj_2_288(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_288() || jj_done);
  }

 inline bool jj_2_289(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_289() || jj_done);
  }

 inline bool jj_2_290(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_290() || jj_done);
  }

 inline bool jj_2_291(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_291() || jj_done);
  }

 inline bool jj_2_292(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_292() || jj_done);
  }

 inline bool jj_2_293(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_293() || jj_done);
  }

 inline bool jj_2_294(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_294() || jj_done);
  }

 inline bool jj_2_295(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_295() || jj_done);
  }

 inline bool jj_2_296(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_296() || jj_done);
  }

 inline bool jj_2_297(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_297() || jj_done);
  }

 inline bool jj_2_298(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_298() || jj_done);
  }

 inline bool jj_2_299(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_299() || jj_done);
  }

 inline bool jj_2_300(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_300() || jj_done);
  }

 inline bool jj_2_301(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_301() || jj_done);
  }

 inline bool jj_2_302(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_302() || jj_done);
  }

 inline bool jj_2_303(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_303() || jj_done);
  }

 inline bool jj_2_304(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_304() || jj_done);
  }

 inline bool jj_2_305(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_305() || jj_done);
  }

 inline bool jj_2_306(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_306() || jj_done);
  }

 inline bool jj_2_307(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_307() || jj_done);
  }

 inline bool jj_2_308(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_308() || jj_done);
  }

 inline bool jj_2_309(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_309() || jj_done);
  }

 inline bool jj_2_310(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_310() || jj_done);
  }

 inline bool jj_2_311(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_311() || jj_done);
  }

 inline bool jj_2_312(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_312() || jj_done);
  }

 inline bool jj_2_313(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_313() || jj_done);
  }

 inline bool jj_2_314(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_314() || jj_done);
  }

 inline bool jj_2_315(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_315() || jj_done);
  }

 inline bool jj_2_316(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_316() || jj_done);
  }

 inline bool jj_2_317(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_317() || jj_done);
  }

 inline bool jj_2_318(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_318() || jj_done);
  }

 inline bool jj_2_319(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_319() || jj_done);
  }

 inline bool jj_2_320(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_320() || jj_done);
  }

 inline bool jj_2_321(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_321() || jj_done);
  }

 inline bool jj_2_322(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_322() || jj_done);
  }

 inline bool jj_2_323(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_323() || jj_done);
  }

 inline bool jj_2_324(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_324() || jj_done);
  }

 inline bool jj_2_325(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_325() || jj_done);
  }

 inline bool jj_2_326(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_326() || jj_done);
  }

 inline bool jj_2_327(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_327() || jj_done);
  }

 inline bool jj_2_328(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_328() || jj_done);
  }

 inline bool jj_2_329(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_329() || jj_done);
  }

 inline bool jj_2_330(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_330() || jj_done);
  }

 inline bool jj_2_331(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_331() || jj_done);
  }

 inline bool jj_2_332(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_332() || jj_done);
  }

 inline bool jj_2_333(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_333() || jj_done);
  }

 inline bool jj_2_334(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_334() || jj_done);
  }

 inline bool jj_2_335(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_335() || jj_done);
  }

 inline bool jj_2_336(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_336() || jj_done);
  }

 inline bool jj_2_337(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_337() || jj_done);
  }

 inline bool jj_2_338(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_338() || jj_done);
  }

 inline bool jj_2_339(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_339() || jj_done);
  }

 inline bool jj_2_340(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_340() || jj_done);
  }

 inline bool jj_2_341(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_341() || jj_done);
  }

 inline bool jj_2_342(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_342() || jj_done);
  }

 inline bool jj_2_343(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_343() || jj_done);
  }

 inline bool jj_2_344(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_344() || jj_done);
  }

 inline bool jj_2_345(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_345() || jj_done);
  }

 inline bool jj_2_346(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_346() || jj_done);
  }

 inline bool jj_2_347(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_347() || jj_done);
  }

 inline bool jj_2_348(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_348() || jj_done);
  }

 inline bool jj_2_349(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_349() || jj_done);
  }

 inline bool jj_2_350(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_350() || jj_done);
  }

 inline bool jj_2_351(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_351() || jj_done);
  }

 inline bool jj_2_352(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_352() || jj_done);
  }

 inline bool jj_2_353(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_353() || jj_done);
  }

 inline bool jj_2_354(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_354() || jj_done);
  }

 inline bool jj_2_355(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_355() || jj_done);
  }

 inline bool jj_2_356(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_356() || jj_done);
  }

 inline bool jj_2_357(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_357() || jj_done);
  }

 inline bool jj_2_358(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_358() || jj_done);
  }

 inline bool jj_2_359(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_359() || jj_done);
  }

 inline bool jj_2_360(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_360() || jj_done);
  }

 inline bool jj_2_361(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_361() || jj_done);
  }

 inline bool jj_2_362(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_362() || jj_done);
  }

 inline bool jj_2_363(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_363() || jj_done);
  }

 inline bool jj_2_364(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_364() || jj_done);
  }

 inline bool jj_2_365(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_365() || jj_done);
  }

 inline bool jj_2_366(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_366() || jj_done);
  }

 inline bool jj_2_367(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_367() || jj_done);
  }

 inline bool jj_2_368(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_368() || jj_done);
  }

 inline bool jj_2_369(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_369() || jj_done);
  }

 inline bool jj_2_370(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_370() || jj_done);
  }

 inline bool jj_2_371(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_371() || jj_done);
  }

 inline bool jj_2_372(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_372() || jj_done);
  }

 inline bool jj_2_373(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_373() || jj_done);
  }

 inline bool jj_2_374(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_374() || jj_done);
  }

 inline bool jj_2_375(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_375() || jj_done);
  }

 inline bool jj_2_376(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_376() || jj_done);
  }

 inline bool jj_2_377(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_377() || jj_done);
  }

 inline bool jj_2_378(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_378() || jj_done);
  }

 inline bool jj_2_379(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_379() || jj_done);
  }

 inline bool jj_2_380(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_380() || jj_done);
  }

 inline bool jj_2_381(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_381() || jj_done);
  }

 inline bool jj_2_382(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_382() || jj_done);
  }

 inline bool jj_2_383(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_383() || jj_done);
  }

 inline bool jj_2_384(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_384() || jj_done);
  }

 inline bool jj_2_385(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_385() || jj_done);
  }

 inline bool jj_2_386(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_386() || jj_done);
  }

 inline bool jj_2_387(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_387() || jj_done);
  }

 inline bool jj_2_388(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_388() || jj_done);
  }

 inline bool jj_2_389(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_389() || jj_done);
  }

 inline bool jj_2_390(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_390() || jj_done);
  }

 inline bool jj_2_391(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_391() || jj_done);
  }

 inline bool jj_2_392(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_392() || jj_done);
  }

 inline bool jj_2_393(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_393() || jj_done);
  }

 inline bool jj_2_394(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_394() || jj_done);
  }

 inline bool jj_2_395(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_395() || jj_done);
  }

 inline bool jj_2_396(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_396() || jj_done);
  }

 inline bool jj_2_397(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_397() || jj_done);
  }

 inline bool jj_2_398(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_398() || jj_done);
  }

 inline bool jj_2_399(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_399() || jj_done);
  }

 inline bool jj_2_400(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_400() || jj_done);
  }

 inline bool jj_2_401(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_401() || jj_done);
  }

 inline bool jj_2_402(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_402() || jj_done);
  }

 inline bool jj_2_403(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_403() || jj_done);
  }

 inline bool jj_2_404(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_404() || jj_done);
  }

 inline bool jj_2_405(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_405() || jj_done);
  }

 inline bool jj_2_406(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_406() || jj_done);
  }

 inline bool jj_2_407(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_407() || jj_done);
  }

 inline bool jj_2_408(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_408() || jj_done);
  }

 inline bool jj_2_409(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_409() || jj_done);
  }

 inline bool jj_2_410(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_410() || jj_done);
  }

 inline bool jj_2_411(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_411() || jj_done);
  }

 inline bool jj_2_412(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_412() || jj_done);
  }

 inline bool jj_2_413(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_413() || jj_done);
  }

 inline bool jj_2_414(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_414() || jj_done);
  }

 inline bool jj_2_415(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_415() || jj_done);
  }

 inline bool jj_2_416(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_416() || jj_done);
  }

 inline bool jj_2_417(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_417() || jj_done);
  }

 inline bool jj_2_418(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_418() || jj_done);
  }

 inline bool jj_2_419(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_419() || jj_done);
  }

 inline bool jj_2_420(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_420() || jj_done);
  }

 inline bool jj_2_421(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_421() || jj_done);
  }

 inline bool jj_2_422(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_422() || jj_done);
  }

 inline bool jj_2_423(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_423() || jj_done);
  }

 inline bool jj_2_424(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_424() || jj_done);
  }

 inline bool jj_2_425(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_425() || jj_done);
  }

 inline bool jj_2_426(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_426() || jj_done);
  }

 inline bool jj_2_427(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_427() || jj_done);
  }

 inline bool jj_2_428(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_428() || jj_done);
  }

 inline bool jj_2_429(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_429() || jj_done);
  }

 inline bool jj_2_430(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_430() || jj_done);
  }

 inline bool jj_2_431(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_431() || jj_done);
  }

 inline bool jj_2_432(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_432() || jj_done);
  }

 inline bool jj_2_433(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_433() || jj_done);
  }

 inline bool jj_2_434(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_434() || jj_done);
  }

 inline bool jj_2_435(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_435() || jj_done);
  }

 inline bool jj_2_436(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_436() || jj_done);
  }

 inline bool jj_2_437(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_437() || jj_done);
  }

 inline bool jj_2_438(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_438() || jj_done);
  }

 inline bool jj_2_439(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_439() || jj_done);
  }

 inline bool jj_2_440(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_440() || jj_done);
  }

 inline bool jj_2_441(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_441() || jj_done);
  }

 inline bool jj_2_442(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_442() || jj_done);
  }

 inline bool jj_2_443(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_443() || jj_done);
  }

 inline bool jj_2_444(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_444() || jj_done);
  }

 inline bool jj_2_445(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_445() || jj_done);
  }

 inline bool jj_2_446(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_446() || jj_done);
  }

 inline bool jj_2_447(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_447() || jj_done);
  }

 inline bool jj_2_448(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_448() || jj_done);
  }

 inline bool jj_2_449(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_449() || jj_done);
  }

 inline bool jj_2_450(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_450() || jj_done);
  }

 inline bool jj_2_451(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_451() || jj_done);
  }

 inline bool jj_2_452(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_452() || jj_done);
  }

 inline bool jj_2_453(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_453() || jj_done);
  }

 inline bool jj_2_454(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_454() || jj_done);
  }

 inline bool jj_2_455(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_455() || jj_done);
  }

 inline bool jj_2_456(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_456() || jj_done);
  }

 inline bool jj_2_457(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_457() || jj_done);
  }

 inline bool jj_2_458(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_458() || jj_done);
  }

 inline bool jj_2_459(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_459() || jj_done);
  }

 inline bool jj_2_460(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_460() || jj_done);
  }

 inline bool jj_2_461(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_461() || jj_done);
  }

 inline bool jj_2_462(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_462() || jj_done);
  }

 inline bool jj_2_463(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_463() || jj_done);
  }

 inline bool jj_2_464(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_464() || jj_done);
  }

 inline bool jj_2_465(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_465() || jj_done);
  }

 inline bool jj_2_466(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_466() || jj_done);
  }

 inline bool jj_2_467(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_467() || jj_done);
  }

 inline bool jj_2_468(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_468() || jj_done);
  }

 inline bool jj_2_469(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_469() || jj_done);
  }

 inline bool jj_2_470(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_470() || jj_done);
  }

 inline bool jj_2_471(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_471() || jj_done);
  }

 inline bool jj_2_472(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_472() || jj_done);
  }

 inline bool jj_2_473(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_473() || jj_done);
  }

 inline bool jj_2_474(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_474() || jj_done);
  }

 inline bool jj_2_475(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_475() || jj_done);
  }

 inline bool jj_2_476(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_476() || jj_done);
  }

 inline bool jj_2_477(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_477() || jj_done);
  }

 inline bool jj_2_478(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_478() || jj_done);
  }

 inline bool jj_2_479(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_479() || jj_done);
  }

 inline bool jj_2_480(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_480() || jj_done);
  }

 inline bool jj_2_481(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_481() || jj_done);
  }

 inline bool jj_2_482(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_482() || jj_done);
  }

 inline bool jj_2_483(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_483() || jj_done);
  }

 inline bool jj_2_484(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_484() || jj_done);
  }

 inline bool jj_2_485(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_485() || jj_done);
  }

 inline bool jj_2_486(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_486() || jj_done);
  }

 inline bool jj_2_487(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_487() || jj_done);
  }

 inline bool jj_2_488(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_488() || jj_done);
  }

 inline bool jj_2_489(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_489() || jj_done);
  }

 inline bool jj_2_490(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_490() || jj_done);
  }

 inline bool jj_2_491(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_491() || jj_done);
  }

 inline bool jj_2_492(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_492() || jj_done);
  }

 inline bool jj_2_493(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_493() || jj_done);
  }

 inline bool jj_2_494(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_494() || jj_done);
  }

 inline bool jj_2_495(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_495() || jj_done);
  }

 inline bool jj_2_496(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_496() || jj_done);
  }

 inline bool jj_2_497(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_497() || jj_done);
  }

 inline bool jj_2_498(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_498() || jj_done);
  }

 inline bool jj_2_499(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_499() || jj_done);
  }

 inline bool jj_2_500(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_500() || jj_done);
  }

 inline bool jj_2_501(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_501() || jj_done);
  }

 inline bool jj_2_502(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_502() || jj_done);
  }

 inline bool jj_2_503(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_503() || jj_done);
  }

 inline bool jj_2_504(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_504() || jj_done);
  }

 inline bool jj_2_505(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_505() || jj_done);
  }

 inline bool jj_2_506(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_506() || jj_done);
  }

 inline bool jj_2_507(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_507() || jj_done);
  }

 inline bool jj_2_508(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_508() || jj_done);
  }

 inline bool jj_2_509(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_509() || jj_done);
  }

 inline bool jj_2_510(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_510() || jj_done);
  }

 inline bool jj_2_511(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_511() || jj_done);
  }

 inline bool jj_2_512(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_512() || jj_done);
  }

 inline bool jj_2_513(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_513() || jj_done);
  }

 inline bool jj_2_514(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_514() || jj_done);
  }

 inline bool jj_2_515(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_515() || jj_done);
  }

 inline bool jj_2_516(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_516() || jj_done);
  }

 inline bool jj_2_517(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_517() || jj_done);
  }

 inline bool jj_2_518(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_518() || jj_done);
  }

 inline bool jj_2_519(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_519() || jj_done);
  }

 inline bool jj_2_520(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_520() || jj_done);
  }

 inline bool jj_2_521(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_521() || jj_done);
  }

 inline bool jj_2_522(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_522() || jj_done);
  }

 inline bool jj_2_523(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_523() || jj_done);
  }

 inline bool jj_2_524(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_524() || jj_done);
  }

 inline bool jj_2_525(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_525() || jj_done);
  }

 inline bool jj_2_526(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_526() || jj_done);
  }

 inline bool jj_2_527(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_527() || jj_done);
  }

 inline bool jj_2_528(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_528() || jj_done);
  }

 inline bool jj_2_529(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_529() || jj_done);
  }

 inline bool jj_2_530(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_530() || jj_done);
  }

 inline bool jj_2_531(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_531() || jj_done);
  }

 inline bool jj_2_532(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_532() || jj_done);
  }

 inline bool jj_2_533(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_533() || jj_done);
  }

 inline bool jj_2_534(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_534() || jj_done);
  }

 inline bool jj_2_535(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_535() || jj_done);
  }

 inline bool jj_2_536(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_536() || jj_done);
  }

 inline bool jj_2_537(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_537() || jj_done);
  }

 inline bool jj_2_538(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_538() || jj_done);
  }

 inline bool jj_2_539(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_539() || jj_done);
  }

 inline bool jj_2_540(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_540() || jj_done);
  }

 inline bool jj_2_541(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_541() || jj_done);
  }

 inline bool jj_2_542(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_542() || jj_done);
  }

 inline bool jj_2_543(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_543() || jj_done);
  }

 inline bool jj_2_544(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_544() || jj_done);
  }

 inline bool jj_2_545(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_545() || jj_done);
  }

 inline bool jj_2_546(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_546() || jj_done);
  }

 inline bool jj_2_547(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_547() || jj_done);
  }

 inline bool jj_2_548(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_548() || jj_done);
  }

 inline bool jj_2_549(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_549() || jj_done);
  }

 inline bool jj_2_550(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_550() || jj_done);
  }

 inline bool jj_2_551(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_551() || jj_done);
  }

 inline bool jj_2_552(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_552() || jj_done);
  }

 inline bool jj_2_553(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_553() || jj_done);
  }

 inline bool jj_2_554(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_554() || jj_done);
  }

 inline bool jj_2_555(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_555() || jj_done);
  }

 inline bool jj_2_556(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_556() || jj_done);
  }

 inline bool jj_2_557(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_557() || jj_done);
  }

 inline bool jj_2_558(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_558() || jj_done);
  }

 inline bool jj_2_559(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_559() || jj_done);
  }

 inline bool jj_2_560(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_560() || jj_done);
  }

 inline bool jj_2_561(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_561() || jj_done);
  }

 inline bool jj_2_562(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_562() || jj_done);
  }

 inline bool jj_2_563(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_563() || jj_done);
  }

 inline bool jj_2_564(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_564() || jj_done);
  }

 inline bool jj_2_565(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_565() || jj_done);
  }

 inline bool jj_2_566(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_566() || jj_done);
  }

 inline bool jj_2_567(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_567() || jj_done);
  }

 inline bool jj_2_568(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_568() || jj_done);
  }

 inline bool jj_2_569(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_569() || jj_done);
  }

 inline bool jj_2_570(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_570() || jj_done);
  }

 inline bool jj_2_571(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_571() || jj_done);
  }

 inline bool jj_2_572(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_572() || jj_done);
  }

 inline bool jj_2_573(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_573() || jj_done);
  }

 inline bool jj_2_574(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_574() || jj_done);
  }

 inline bool jj_2_575(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_575() || jj_done);
  }

 inline bool jj_2_576(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_576() || jj_done);
  }

 inline bool jj_2_577(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_577() || jj_done);
  }

 inline bool jj_2_578(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_578() || jj_done);
  }

 inline bool jj_2_579(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_579() || jj_done);
  }

 inline bool jj_2_580(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_580() || jj_done);
  }

 inline bool jj_2_581(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_581() || jj_done);
  }

 inline bool jj_2_582(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_582() || jj_done);
  }

 inline bool jj_2_583(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_583() || jj_done);
  }

 inline bool jj_2_584(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_584() || jj_done);
  }

 inline bool jj_2_585(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_585() || jj_done);
  }

 inline bool jj_2_586(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_586() || jj_done);
  }

 inline bool jj_2_587(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_587() || jj_done);
  }

 inline bool jj_2_588(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_588() || jj_done);
  }

 inline bool jj_2_589(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_589() || jj_done);
  }

 inline bool jj_2_590(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_590() || jj_done);
  }

 inline bool jj_2_591(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_591() || jj_done);
  }

 inline bool jj_2_592(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_592() || jj_done);
  }

 inline bool jj_2_593(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_593() || jj_done);
  }

 inline bool jj_2_594(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_594() || jj_done);
  }

 inline bool jj_2_595(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_595() || jj_done);
  }

 inline bool jj_2_596(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_596() || jj_done);
  }

 inline bool jj_2_597(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_597() || jj_done);
  }

 inline bool jj_2_598(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_598() || jj_done);
  }

 inline bool jj_2_599(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_599() || jj_done);
  }

 inline bool jj_2_600(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_600() || jj_done);
  }

 inline bool jj_2_601(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_601() || jj_done);
  }

 inline bool jj_2_602(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_602() || jj_done);
  }

 inline bool jj_2_603(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_603() || jj_done);
  }

 inline bool jj_2_604(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_604() || jj_done);
  }

 inline bool jj_2_605(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_605() || jj_done);
  }

 inline bool jj_2_606(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_606() || jj_done);
  }

 inline bool jj_2_607(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_607() || jj_done);
  }

 inline bool jj_2_608(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_608() || jj_done);
  }

 inline bool jj_2_609(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_609() || jj_done);
  }

 inline bool jj_2_610(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_610() || jj_done);
  }

 inline bool jj_2_611(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_611() || jj_done);
  }

 inline bool jj_2_612(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_612() || jj_done);
  }

 inline bool jj_2_613(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_613() || jj_done);
  }

 inline bool jj_2_614(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_614() || jj_done);
  }

 inline bool jj_2_615(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_615() || jj_done);
  }

 inline bool jj_2_616(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_616() || jj_done);
  }

 inline bool jj_2_617(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_617() || jj_done);
  }

 inline bool jj_2_618(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_618() || jj_done);
  }

 inline bool jj_2_619(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_619() || jj_done);
  }

 inline bool jj_2_620(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_620() || jj_done);
  }

 inline bool jj_2_621(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_621() || jj_done);
  }

 inline bool jj_2_622(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_622() || jj_done);
  }

 inline bool jj_2_623(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_623() || jj_done);
  }

 inline bool jj_2_624(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_624() || jj_done);
  }

 inline bool jj_2_625(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_625() || jj_done);
  }

 inline bool jj_2_626(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_626() || jj_done);
  }

 inline bool jj_2_627(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_627() || jj_done);
  }

 inline bool jj_2_628(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_628() || jj_done);
  }

 inline bool jj_2_629(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_629() || jj_done);
  }

 inline bool jj_2_630(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_630() || jj_done);
  }

 inline bool jj_2_631(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_631() || jj_done);
  }

 inline bool jj_2_632(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_632() || jj_done);
  }

 inline bool jj_2_633(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_633() || jj_done);
  }

 inline bool jj_2_634(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_634() || jj_done);
  }

 inline bool jj_2_635(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_635() || jj_done);
  }

 inline bool jj_2_636(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_636() || jj_done);
  }

 inline bool jj_2_637(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_637() || jj_done);
  }

 inline bool jj_2_638(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_638() || jj_done);
  }

 inline bool jj_2_639(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_639() || jj_done);
  }

 inline bool jj_2_640(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_640() || jj_done);
  }

 inline bool jj_2_641(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_641() || jj_done);
  }

 inline bool jj_2_642(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_642() || jj_done);
  }

 inline bool jj_2_643(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_643() || jj_done);
  }

 inline bool jj_2_644(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_644() || jj_done);
  }

 inline bool jj_2_645(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_645() || jj_done);
  }

 inline bool jj_2_646(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_646() || jj_done);
  }

 inline bool jj_2_647(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_647() || jj_done);
  }

 inline bool jj_2_648(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_648() || jj_done);
  }

 inline bool jj_2_649(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_649() || jj_done);
  }

 inline bool jj_2_650(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_650() || jj_done);
  }

 inline bool jj_2_651(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_651() || jj_done);
  }

 inline bool jj_2_652(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_652() || jj_done);
  }

 inline bool jj_2_653(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_653() || jj_done);
  }

 inline bool jj_2_654(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_654() || jj_done);
  }

 inline bool jj_2_655(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_655() || jj_done);
  }

 inline bool jj_2_656(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_656() || jj_done);
  }

 inline bool jj_2_657(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_657() || jj_done);
  }

 inline bool jj_2_658(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_658() || jj_done);
  }

 inline bool jj_2_659(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_659() || jj_done);
  }

 inline bool jj_2_660(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_660() || jj_done);
  }

 inline bool jj_2_661(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_661() || jj_done);
  }

 inline bool jj_2_662(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_662() || jj_done);
  }

 inline bool jj_2_663(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_663() || jj_done);
  }

 inline bool jj_2_664(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_664() || jj_done);
  }

 inline bool jj_2_665(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_665() || jj_done);
  }

 inline bool jj_2_666(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_666() || jj_done);
  }

 inline bool jj_2_667(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_667() || jj_done);
  }

 inline bool jj_2_668(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_668() || jj_done);
  }

 inline bool jj_2_669(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_669() || jj_done);
  }

 inline bool jj_2_670(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_670() || jj_done);
  }

 inline bool jj_2_671(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_671() || jj_done);
  }

 inline bool jj_2_672(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_672() || jj_done);
  }

 inline bool jj_2_673(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_673() || jj_done);
  }

 inline bool jj_2_674(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_674() || jj_done);
  }

 inline bool jj_2_675(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_675() || jj_done);
  }

 inline bool jj_2_676(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_676() || jj_done);
  }

 inline bool jj_2_677(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_677() || jj_done);
  }

 inline bool jj_2_678(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_678() || jj_done);
  }

 inline bool jj_2_679(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_679() || jj_done);
  }

 inline bool jj_2_680(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_680() || jj_done);
  }

 inline bool jj_2_681(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_681() || jj_done);
  }

 inline bool jj_2_682(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_682() || jj_done);
  }

 inline bool jj_2_683(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_683() || jj_done);
  }

 inline bool jj_2_684(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_684() || jj_done);
  }

 inline bool jj_2_685(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_685() || jj_done);
  }

 inline bool jj_2_686(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_686() || jj_done);
  }

 inline bool jj_2_687(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_687() || jj_done);
  }

 inline bool jj_2_688(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_688() || jj_done);
  }

 inline bool jj_2_689(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_689() || jj_done);
  }

 inline bool jj_2_690(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_690() || jj_done);
  }

 inline bool jj_2_691(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_691() || jj_done);
  }

 inline bool jj_2_692(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_692() || jj_done);
  }

 inline bool jj_2_693(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_693() || jj_done);
  }

 inline bool jj_2_694(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_694() || jj_done);
  }

 inline bool jj_2_695(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_695() || jj_done);
  }

 inline bool jj_2_696(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_696() || jj_done);
  }

 inline bool jj_2_697(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_697() || jj_done);
  }

 inline bool jj_2_698(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_698() || jj_done);
  }

 inline bool jj_2_699(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_699() || jj_done);
  }

 inline bool jj_2_700(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_700() || jj_done);
  }

 inline bool jj_2_701(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_701() || jj_done);
  }

 inline bool jj_2_702(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_702() || jj_done);
  }

 inline bool jj_2_703(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_703() || jj_done);
  }

 inline bool jj_2_704(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_704() || jj_done);
  }

 inline bool jj_2_705(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_705() || jj_done);
  }

 inline bool jj_2_706(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_706() || jj_done);
  }

 inline bool jj_2_707(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_707() || jj_done);
  }

 inline bool jj_2_708(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_708() || jj_done);
  }

 inline bool jj_2_709(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_709() || jj_done);
  }

 inline bool jj_2_710(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_710() || jj_done);
  }

 inline bool jj_2_711(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_711() || jj_done);
  }

 inline bool jj_2_712(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_712() || jj_done);
  }

 inline bool jj_2_713(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_713() || jj_done);
  }

 inline bool jj_2_714(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_714() || jj_done);
  }

 inline bool jj_2_715(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_715() || jj_done);
  }

 inline bool jj_2_716(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_716() || jj_done);
  }

 inline bool jj_2_717(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_717() || jj_done);
  }

 inline bool jj_2_718(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_718() || jj_done);
  }

 inline bool jj_2_719(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_719() || jj_done);
  }

 inline bool jj_2_720(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_720() || jj_done);
  }

 inline bool jj_2_721(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_721() || jj_done);
  }

 inline bool jj_2_722(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_722() || jj_done);
  }

 inline bool jj_2_723(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_723() || jj_done);
  }

 inline bool jj_2_724(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_724() || jj_done);
  }

 inline bool jj_2_725(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_725() || jj_done);
  }

 inline bool jj_2_726(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_726() || jj_done);
  }

 inline bool jj_2_727(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_727() || jj_done);
  }

 inline bool jj_2_728(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_728() || jj_done);
  }

 inline bool jj_2_729(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_729() || jj_done);
  }

 inline bool jj_2_730(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_730() || jj_done);
  }

 inline bool jj_2_731(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_731() || jj_done);
  }

 inline bool jj_2_732(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_732() || jj_done);
  }

 inline bool jj_2_733(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_733() || jj_done);
  }

 inline bool jj_2_734(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_734() || jj_done);
  }

 inline bool jj_2_735(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_735() || jj_done);
  }

 inline bool jj_2_736(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_736() || jj_done);
  }

 inline bool jj_2_737(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_737() || jj_done);
  }

 inline bool jj_2_738(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_738() || jj_done);
  }

 inline bool jj_2_739(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_739() || jj_done);
  }

 inline bool jj_2_740(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_740() || jj_done);
  }

 inline bool jj_2_741(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_741() || jj_done);
  }

 inline bool jj_2_742(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_742() || jj_done);
  }

 inline bool jj_2_743(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_743() || jj_done);
  }

 inline bool jj_2_744(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_744() || jj_done);
  }

 inline bool jj_2_745(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_745() || jj_done);
  }

 inline bool jj_2_746(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_746() || jj_done);
  }

 inline bool jj_2_747(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_747() || jj_done);
  }

 inline bool jj_2_748(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_748() || jj_done);
  }

 inline bool jj_2_749(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_749() || jj_done);
  }

 inline bool jj_2_750(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_750() || jj_done);
  }

 inline bool jj_2_751(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_751() || jj_done);
  }

 inline bool jj_2_752(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_752() || jj_done);
  }

 inline bool jj_2_753(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_753() || jj_done);
  }

 inline bool jj_2_754(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_754() || jj_done);
  }

 inline bool jj_2_755(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_755() || jj_done);
  }

 inline bool jj_2_756(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_756() || jj_done);
  }

 inline bool jj_2_757(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_757() || jj_done);
  }

 inline bool jj_2_758(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_758() || jj_done);
  }

 inline bool jj_2_759(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_759() || jj_done);
  }

 inline bool jj_2_760(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_760() || jj_done);
  }

 inline bool jj_2_761(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_761() || jj_done);
  }

 inline bool jj_2_762(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_762() || jj_done);
  }

 inline bool jj_2_763(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_763() || jj_done);
  }

 inline bool jj_2_764(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_764() || jj_done);
  }

 inline bool jj_2_765(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_765() || jj_done);
  }

 inline bool jj_2_766(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_766() || jj_done);
  }

 inline bool jj_2_767(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_767() || jj_done);
  }

 inline bool jj_2_768(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_768() || jj_done);
  }

 inline bool jj_2_769(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_769() || jj_done);
  }

 inline bool jj_2_770(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_770() || jj_done);
  }

 inline bool jj_2_771(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_771() || jj_done);
  }

 inline bool jj_2_772(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_772() || jj_done);
  }

 inline bool jj_2_773(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_773() || jj_done);
  }

 inline bool jj_2_774(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_774() || jj_done);
  }

 inline bool jj_2_775(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_775() || jj_done);
  }

 inline bool jj_2_776(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_776() || jj_done);
  }

 inline bool jj_2_777(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_777() || jj_done);
  }

 inline bool jj_2_778(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_778() || jj_done);
  }

 inline bool jj_2_779(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_779() || jj_done);
  }

 inline bool jj_2_780(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_780() || jj_done);
  }

 inline bool jj_2_781(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_781() || jj_done);
  }

 inline bool jj_2_782(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_782() || jj_done);
  }

 inline bool jj_2_783(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_783() || jj_done);
  }

 inline bool jj_2_784(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_784() || jj_done);
  }

 inline bool jj_2_785(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_785() || jj_done);
  }

 inline bool jj_2_786(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_786() || jj_done);
  }

 inline bool jj_2_787(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_787() || jj_done);
  }

 inline bool jj_2_788(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_788() || jj_done);
  }

 inline bool jj_2_789(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_789() || jj_done);
  }

 inline bool jj_2_790(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_790() || jj_done);
  }

 inline bool jj_2_791(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_791() || jj_done);
  }

 inline bool jj_2_792(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_792() || jj_done);
  }

 inline bool jj_2_793(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_793() || jj_done);
  }

 inline bool jj_2_794(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_794() || jj_done);
  }

 inline bool jj_2_795(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_795() || jj_done);
  }

 inline bool jj_2_796(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_796() || jj_done);
  }

 inline bool jj_2_797(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_797() || jj_done);
  }

 inline bool jj_2_798(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_798() || jj_done);
  }

 inline bool jj_2_799(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_799() || jj_done);
  }

 inline bool jj_2_800(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_800() || jj_done);
  }

 inline bool jj_2_801(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_801() || jj_done);
  }

 inline bool jj_2_802(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_802() || jj_done);
  }

 inline bool jj_2_803(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_803() || jj_done);
  }

 inline bool jj_2_804(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_804() || jj_done);
  }

 inline bool jj_2_805(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_805() || jj_done);
  }

 inline bool jj_2_806(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_806() || jj_done);
  }

 inline bool jj_2_807(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_807() || jj_done);
  }

 inline bool jj_2_808(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_808() || jj_done);
  }

 inline bool jj_2_809(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_809() || jj_done);
  }

 inline bool jj_2_810(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_810() || jj_done);
  }

 inline bool jj_2_811(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_811() || jj_done);
  }

 inline bool jj_2_812(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_812() || jj_done);
  }

 inline bool jj_2_813(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_813() || jj_done);
  }

 inline bool jj_2_814(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_814() || jj_done);
  }

 inline bool jj_2_815(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_815() || jj_done);
  }

 inline bool jj_2_816(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_816() || jj_done);
  }

 inline bool jj_2_817(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_817() || jj_done);
  }

 inline bool jj_2_818(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_818() || jj_done);
  }

 inline bool jj_2_819(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_819() || jj_done);
  }

 inline bool jj_2_820(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_820() || jj_done);
  }

 inline bool jj_2_821(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_821() || jj_done);
  }

 inline bool jj_2_822(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_822() || jj_done);
  }

 inline bool jj_2_823(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_823() || jj_done);
  }

 inline bool jj_2_824(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_824() || jj_done);
  }

 inline bool jj_2_825(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_825() || jj_done);
  }

 inline bool jj_2_826(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_826() || jj_done);
  }

 inline bool jj_2_827(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_827() || jj_done);
  }

 inline bool jj_2_828(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_828() || jj_done);
  }

 inline bool jj_2_829(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_829() || jj_done);
  }

 inline bool jj_2_830(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_830() || jj_done);
  }

 inline bool jj_2_831(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_831() || jj_done);
  }

 inline bool jj_2_832(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_832() || jj_done);
  }

 inline bool jj_2_833(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_833() || jj_done);
  }

 inline bool jj_2_834(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_834() || jj_done);
  }

 inline bool jj_2_835(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_835() || jj_done);
  }

 inline bool jj_2_836(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_836() || jj_done);
  }

 inline bool jj_2_837(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_837() || jj_done);
  }

 inline bool jj_2_838(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_838() || jj_done);
  }

 inline bool jj_2_839(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_839() || jj_done);
  }

 inline bool jj_2_840(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_840() || jj_done);
  }

 inline bool jj_2_841(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_841() || jj_done);
  }

 inline bool jj_2_842(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_842() || jj_done);
  }

 inline bool jj_2_843(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_843() || jj_done);
  }

 inline bool jj_2_844(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_844() || jj_done);
  }

 inline bool jj_2_845(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_845() || jj_done);
  }

 inline bool jj_2_846(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_846() || jj_done);
  }

 inline bool jj_2_847(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_847() || jj_done);
  }

 inline bool jj_2_848(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_848() || jj_done);
  }

 inline bool jj_2_849(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_849() || jj_done);
  }

 inline bool jj_2_850(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_850() || jj_done);
  }

 inline bool jj_2_851(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_851() || jj_done);
  }

 inline bool jj_2_852(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_852() || jj_done);
  }

 inline bool jj_2_853(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_853() || jj_done);
  }

 inline bool jj_2_854(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_854() || jj_done);
  }

 inline bool jj_2_855(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_855() || jj_done);
  }

 inline bool jj_2_856(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_856() || jj_done);
  }

 inline bool jj_2_857(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_857() || jj_done);
  }

 inline bool jj_2_858(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_858() || jj_done);
  }

 inline bool jj_2_859(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_859() || jj_done);
  }

 inline bool jj_2_860(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_860() || jj_done);
  }

 inline bool jj_2_861(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_861() || jj_done);
  }

 inline bool jj_2_862(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_862() || jj_done);
  }

 inline bool jj_2_863(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_863() || jj_done);
  }

 inline bool jj_2_864(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_864() || jj_done);
  }

 inline bool jj_2_865(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_865() || jj_done);
  }

 inline bool jj_2_866(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_866() || jj_done);
  }

 inline bool jj_2_867(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_867() || jj_done);
  }

 inline bool jj_2_868(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_868() || jj_done);
  }

 inline bool jj_2_869(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_869() || jj_done);
  }

 inline bool jj_2_870(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_870() || jj_done);
  }

 inline bool jj_2_871(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_871() || jj_done);
  }

 inline bool jj_2_872(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_872() || jj_done);
  }

 inline bool jj_2_873(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_873() || jj_done);
  }

 inline bool jj_2_874(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_874() || jj_done);
  }

 inline bool jj_2_875(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_875() || jj_done);
  }

 inline bool jj_2_876(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_876() || jj_done);
  }

 inline bool jj_2_877(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_877() || jj_done);
  }

 inline bool jj_2_878(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_878() || jj_done);
  }

 inline bool jj_2_879(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_879() || jj_done);
  }

 inline bool jj_2_880(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_880() || jj_done);
  }

 inline bool jj_2_881(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_881() || jj_done);
  }

 inline bool jj_2_882(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_882() || jj_done);
  }

 inline bool jj_2_883(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_883() || jj_done);
  }

 inline bool jj_2_884(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_884() || jj_done);
  }

 inline bool jj_2_885(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_885() || jj_done);
  }

 inline bool jj_2_886(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_886() || jj_done);
  }

 inline bool jj_2_887(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_887() || jj_done);
  }

 inline bool jj_2_888(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_888() || jj_done);
  }

 inline bool jj_2_889(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_889() || jj_done);
  }

 inline bool jj_2_890(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_890() || jj_done);
  }

 inline bool jj_2_891(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_891() || jj_done);
  }

 inline bool jj_2_892(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_892() || jj_done);
  }

 inline bool jj_2_893(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_893() || jj_done);
  }

 inline bool jj_2_894(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_894() || jj_done);
  }

 inline bool jj_2_895(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_895() || jj_done);
  }

 inline bool jj_2_896(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_896() || jj_done);
  }

 inline bool jj_2_897(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_897() || jj_done);
  }

 inline bool jj_2_898(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_898() || jj_done);
  }

 inline bool jj_2_899(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_899() || jj_done);
  }

 inline bool jj_2_900(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_900() || jj_done);
  }

 inline bool jj_2_901(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_901() || jj_done);
  }

 inline bool jj_2_902(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_902() || jj_done);
  }

 inline bool jj_2_903(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_903() || jj_done);
  }

 inline bool jj_2_904(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_904() || jj_done);
  }

 inline bool jj_2_905(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_905() || jj_done);
  }

 inline bool jj_2_906(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_906() || jj_done);
  }

 inline bool jj_2_907(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_907() || jj_done);
  }

 inline bool jj_2_908(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_908() || jj_done);
  }

 inline bool jj_2_909(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_909() || jj_done);
  }

 inline bool jj_2_910(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_910() || jj_done);
  }

 inline bool jj_2_911(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_911() || jj_done);
  }

 inline bool jj_2_912(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_912() || jj_done);
  }

 inline bool jj_2_913(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_913() || jj_done);
  }

 inline bool jj_2_914(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_914() || jj_done);
  }

 inline bool jj_2_915(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_915() || jj_done);
  }

 inline bool jj_2_916(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_916() || jj_done);
  }

 inline bool jj_2_917(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_917() || jj_done);
  }

 inline bool jj_2_918(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_918() || jj_done);
  }

 inline bool jj_2_919(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_919() || jj_done);
  }

 inline bool jj_2_920(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_920() || jj_done);
  }

 inline bool jj_2_921(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_921() || jj_done);
  }

 inline bool jj_2_922(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_922() || jj_done);
  }

 inline bool jj_2_923(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_923() || jj_done);
  }

 inline bool jj_2_924(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_924() || jj_done);
  }

 inline bool jj_2_925(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_925() || jj_done);
  }

 inline bool jj_2_926(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_926() || jj_done);
  }

 inline bool jj_2_927(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_927() || jj_done);
  }

 inline bool jj_2_928(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_928() || jj_done);
  }

 inline bool jj_2_929(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_929() || jj_done);
  }

 inline bool jj_2_930(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_930() || jj_done);
  }

 inline bool jj_2_931(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_931() || jj_done);
  }

 inline bool jj_2_932(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_932() || jj_done);
  }

 inline bool jj_2_933(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_933() || jj_done);
  }

 inline bool jj_2_934(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_934() || jj_done);
  }

 inline bool jj_2_935(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_935() || jj_done);
  }

 inline bool jj_2_936(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_936() || jj_done);
  }

 inline bool jj_2_937(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_937() || jj_done);
  }

 inline bool jj_2_938(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_938() || jj_done);
  }

 inline bool jj_2_939(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_939() || jj_done);
  }

 inline bool jj_2_940(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_940() || jj_done);
  }

 inline bool jj_2_941(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_941() || jj_done);
  }

 inline bool jj_2_942(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_942() || jj_done);
  }

 inline bool jj_2_943(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_943() || jj_done);
  }

 inline bool jj_2_944(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_944() || jj_done);
  }

 inline bool jj_2_945(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_945() || jj_done);
  }

 inline bool jj_2_946(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_946() || jj_done);
  }

 inline bool jj_2_947(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_947() || jj_done);
  }

 inline bool jj_2_948(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_948() || jj_done);
  }

 inline bool jj_2_949(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_949() || jj_done);
  }

 inline bool jj_2_950(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_950() || jj_done);
  }

 inline bool jj_2_951(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_951() || jj_done);
  }

 inline bool jj_2_952(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_952() || jj_done);
  }

 inline bool jj_2_953(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_953() || jj_done);
  }

 inline bool jj_2_954(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_954() || jj_done);
  }

 inline bool jj_2_955(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_955() || jj_done);
  }

 inline bool jj_2_956(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_956() || jj_done);
  }

 inline bool jj_2_957(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_957() || jj_done);
  }

 inline bool jj_2_958(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_958() || jj_done);
  }

 inline bool jj_2_959(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_959() || jj_done);
  }

 inline bool jj_2_960(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_960() || jj_done);
  }

 inline bool jj_2_961(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_961() || jj_done);
  }

 inline bool jj_2_962(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_962() || jj_done);
  }

 inline bool jj_2_963(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_963() || jj_done);
  }

 inline bool jj_2_964(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_964() || jj_done);
  }

 inline bool jj_2_965(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_965() || jj_done);
  }

 inline bool jj_2_966(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_966() || jj_done);
  }

 inline bool jj_2_967(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_967() || jj_done);
  }

 inline bool jj_2_968(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_968() || jj_done);
  }

 inline bool jj_2_969(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_969() || jj_done);
  }

 inline bool jj_2_970(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_970() || jj_done);
  }

 inline bool jj_2_971(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_971() || jj_done);
  }

 inline bool jj_2_972(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_972() || jj_done);
  }

 inline bool jj_2_973(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_973() || jj_done);
  }

 inline bool jj_2_974(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_974() || jj_done);
  }

 inline bool jj_2_975(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_975() || jj_done);
  }

 inline bool jj_2_976(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_976() || jj_done);
  }

 inline bool jj_2_977(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_977() || jj_done);
  }

 inline bool jj_2_978(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_978() || jj_done);
  }

 inline bool jj_2_979(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_979() || jj_done);
  }

 inline bool jj_2_980(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_980() || jj_done);
  }

 inline bool jj_2_981(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_981() || jj_done);
  }

 inline bool jj_2_982(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_982() || jj_done);
  }

 inline bool jj_2_983(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_983() || jj_done);
  }

 inline bool jj_2_984(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_984() || jj_done);
  }

 inline bool jj_2_985(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_985() || jj_done);
  }

 inline bool jj_2_986(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_986() || jj_done);
  }

 inline bool jj_2_987(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_987() || jj_done);
  }

 inline bool jj_2_988(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_988() || jj_done);
  }

 inline bool jj_2_989(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_989() || jj_done);
  }

 inline bool jj_2_990(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_990() || jj_done);
  }

 inline bool jj_2_991(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_991() || jj_done);
  }

 inline bool jj_2_992(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_992() || jj_done);
  }

 inline bool jj_2_993(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_993() || jj_done);
  }

 inline bool jj_2_994(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_994() || jj_done);
  }

 inline bool jj_2_995(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_995() || jj_done);
  }

 inline bool jj_2_996(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_996() || jj_done);
  }

 inline bool jj_2_997(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_997() || jj_done);
  }

 inline bool jj_2_998(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_998() || jj_done);
  }

 inline bool jj_2_999(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_999() || jj_done);
  }

 inline bool jj_2_1000(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1000() || jj_done);
  }

 inline bool jj_2_1001(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1001() || jj_done);
  }

 inline bool jj_2_1002(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1002() || jj_done);
  }

 inline bool jj_2_1003(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1003() || jj_done);
  }

 inline bool jj_2_1004(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1004() || jj_done);
  }

 inline bool jj_2_1005(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1005() || jj_done);
  }

 inline bool jj_2_1006(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1006() || jj_done);
  }

 inline bool jj_2_1007(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1007() || jj_done);
  }

 inline bool jj_2_1008(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1008() || jj_done);
  }

 inline bool jj_2_1009(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1009() || jj_done);
  }

 inline bool jj_2_1010(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1010() || jj_done);
  }

 inline bool jj_2_1011(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1011() || jj_done);
  }

 inline bool jj_2_1012(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1012() || jj_done);
  }

 inline bool jj_2_1013(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1013() || jj_done);
  }

 inline bool jj_2_1014(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1014() || jj_done);
  }

 inline bool jj_2_1015(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1015() || jj_done);
  }

 inline bool jj_2_1016(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1016() || jj_done);
  }

 inline bool jj_2_1017(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1017() || jj_done);
  }

 inline bool jj_2_1018(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1018() || jj_done);
  }

 inline bool jj_2_1019(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1019() || jj_done);
  }

 inline bool jj_2_1020(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1020() || jj_done);
  }

 inline bool jj_2_1021(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1021() || jj_done);
  }

 inline bool jj_2_1022(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1022() || jj_done);
  }

 inline bool jj_2_1023(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1023() || jj_done);
  }

 inline bool jj_2_1024(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1024() || jj_done);
  }

 inline bool jj_2_1025(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1025() || jj_done);
  }

 inline bool jj_2_1026(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1026() || jj_done);
  }

 inline bool jj_2_1027(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1027() || jj_done);
  }

 inline bool jj_2_1028(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1028() || jj_done);
  }

 inline bool jj_2_1029(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1029() || jj_done);
  }

 inline bool jj_2_1030(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1030() || jj_done);
  }

 inline bool jj_2_1031(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1031() || jj_done);
  }

 inline bool jj_2_1032(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1032() || jj_done);
  }

 inline bool jj_2_1033(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1033() || jj_done);
  }

 inline bool jj_2_1034(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1034() || jj_done);
  }

 inline bool jj_2_1035(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1035() || jj_done);
  }

 inline bool jj_2_1036(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1036() || jj_done);
  }

 inline bool jj_2_1037(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1037() || jj_done);
  }

 inline bool jj_2_1038(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1038() || jj_done);
  }

 inline bool jj_2_1039(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1039() || jj_done);
  }

 inline bool jj_2_1040(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1040() || jj_done);
  }

 inline bool jj_2_1041(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1041() || jj_done);
  }

 inline bool jj_2_1042(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1042() || jj_done);
  }

 inline bool jj_2_1043(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1043() || jj_done);
  }

 inline bool jj_2_1044(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1044() || jj_done);
  }

 inline bool jj_2_1045(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1045() || jj_done);
  }

 inline bool jj_2_1046(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1046() || jj_done);
  }

 inline bool jj_2_1047(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1047() || jj_done);
  }

 inline bool jj_2_1048(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1048() || jj_done);
  }

 inline bool jj_2_1049(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1049() || jj_done);
  }

 inline bool jj_2_1050(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1050() || jj_done);
  }

 inline bool jj_2_1051(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1051() || jj_done);
  }

 inline bool jj_2_1052(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1052() || jj_done);
  }

 inline bool jj_2_1053(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1053() || jj_done);
  }

 inline bool jj_2_1054(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1054() || jj_done);
  }

 inline bool jj_2_1055(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1055() || jj_done);
  }

 inline bool jj_2_1056(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1056() || jj_done);
  }

 inline bool jj_2_1057(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1057() || jj_done);
  }

 inline bool jj_2_1058(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1058() || jj_done);
  }

 inline bool jj_2_1059(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1059() || jj_done);
  }

 inline bool jj_2_1060(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1060() || jj_done);
  }

 inline bool jj_2_1061(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1061() || jj_done);
  }

 inline bool jj_2_1062(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1062() || jj_done);
  }

 inline bool jj_2_1063(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1063() || jj_done);
  }

 inline bool jj_2_1064(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1064() || jj_done);
  }

 inline bool jj_2_1065(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1065() || jj_done);
  }

 inline bool jj_2_1066(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1066() || jj_done);
  }

 inline bool jj_2_1067(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1067() || jj_done);
  }

 inline bool jj_2_1068(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1068() || jj_done);
  }

 inline bool jj_2_1069(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1069() || jj_done);
  }

 inline bool jj_2_1070(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1070() || jj_done);
  }

 inline bool jj_2_1071(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1071() || jj_done);
  }

 inline bool jj_2_1072(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1072() || jj_done);
  }

 inline bool jj_2_1073(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1073() || jj_done);
  }

 inline bool jj_2_1074(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1074() || jj_done);
  }

 inline bool jj_2_1075(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1075() || jj_done);
  }

 inline bool jj_2_1076(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1076() || jj_done);
  }

 inline bool jj_2_1077(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1077() || jj_done);
  }

 inline bool jj_2_1078(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1078() || jj_done);
  }

 inline bool jj_2_1079(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1079() || jj_done);
  }

 inline bool jj_2_1080(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1080() || jj_done);
  }

 inline bool jj_2_1081(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1081() || jj_done);
  }

 inline bool jj_2_1082(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1082() || jj_done);
  }

 inline bool jj_2_1083(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1083() || jj_done);
  }

 inline bool jj_2_1084(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1084() || jj_done);
  }

 inline bool jj_2_1085(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1085() || jj_done);
  }

 inline bool jj_2_1086(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1086() || jj_done);
  }

 inline bool jj_2_1087(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1087() || jj_done);
  }

 inline bool jj_2_1088(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1088() || jj_done);
  }

 inline bool jj_2_1089(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1089() || jj_done);
  }

 inline bool jj_2_1090(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1090() || jj_done);
  }

 inline bool jj_2_1091(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1091() || jj_done);
  }

 inline bool jj_2_1092(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1092() || jj_done);
  }

 inline bool jj_2_1093(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1093() || jj_done);
  }

 inline bool jj_2_1094(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1094() || jj_done);
  }

 inline bool jj_2_1095(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1095() || jj_done);
  }

 inline bool jj_2_1096(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1096() || jj_done);
  }

 inline bool jj_2_1097(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1097() || jj_done);
  }

 inline bool jj_2_1098(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1098() || jj_done);
  }

 inline bool jj_2_1099(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1099() || jj_done);
  }

 inline bool jj_2_1100(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1100() || jj_done);
  }

 inline bool jj_2_1101(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1101() || jj_done);
  }

 inline bool jj_2_1102(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1102() || jj_done);
  }

 inline bool jj_2_1103(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1103() || jj_done);
  }

 inline bool jj_2_1104(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1104() || jj_done);
  }

 inline bool jj_2_1105(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1105() || jj_done);
  }

 inline bool jj_2_1106(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1106() || jj_done);
  }

 inline bool jj_2_1107(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1107() || jj_done);
  }

 inline bool jj_2_1108(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1108() || jj_done);
  }

 inline bool jj_2_1109(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1109() || jj_done);
  }

 inline bool jj_2_1110(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1110() || jj_done);
  }

 inline bool jj_2_1111(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1111() || jj_done);
  }

 inline bool jj_2_1112(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1112() || jj_done);
  }

 inline bool jj_2_1113(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1113() || jj_done);
  }

 inline bool jj_2_1114(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1114() || jj_done);
  }

 inline bool jj_2_1115(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1115() || jj_done);
  }

 inline bool jj_2_1116(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1116() || jj_done);
  }

 inline bool jj_2_1117(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1117() || jj_done);
  }

 inline bool jj_2_1118(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1118() || jj_done);
  }

 inline bool jj_2_1119(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1119() || jj_done);
  }

 inline bool jj_2_1120(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1120() || jj_done);
  }

 inline bool jj_2_1121(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1121() || jj_done);
  }

 inline bool jj_2_1122(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1122() || jj_done);
  }

 inline bool jj_2_1123(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1123() || jj_done);
  }

 inline bool jj_2_1124(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1124() || jj_done);
  }

 inline bool jj_2_1125(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1125() || jj_done);
  }

 inline bool jj_2_1126(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1126() || jj_done);
  }

 inline bool jj_2_1127(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1127() || jj_done);
  }

 inline bool jj_2_1128(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1128() || jj_done);
  }

 inline bool jj_2_1129(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1129() || jj_done);
  }

 inline bool jj_2_1130(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1130() || jj_done);
  }

 inline bool jj_2_1131(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1131() || jj_done);
  }

 inline bool jj_2_1132(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1132() || jj_done);
  }

 inline bool jj_2_1133(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1133() || jj_done);
  }

 inline bool jj_2_1134(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1134() || jj_done);
  }

 inline bool jj_2_1135(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1135() || jj_done);
  }

 inline bool jj_2_1136(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1136() || jj_done);
  }

 inline bool jj_2_1137(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1137() || jj_done);
  }

 inline bool jj_2_1138(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1138() || jj_done);
  }

 inline bool jj_2_1139(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1139() || jj_done);
  }

 inline bool jj_2_1140(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1140() || jj_done);
  }

 inline bool jj_2_1141(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1141() || jj_done);
  }

 inline bool jj_2_1142(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1142() || jj_done);
  }

 inline bool jj_2_1143(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1143() || jj_done);
  }

 inline bool jj_2_1144(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1144() || jj_done);
  }

 inline bool jj_2_1145(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1145() || jj_done);
  }

 inline bool jj_2_1146(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1146() || jj_done);
  }

 inline bool jj_2_1147(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1147() || jj_done);
  }

 inline bool jj_2_1148(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1148() || jj_done);
  }

 inline bool jj_2_1149(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1149() || jj_done);
  }

 inline bool jj_2_1150(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1150() || jj_done);
  }

 inline bool jj_2_1151(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1151() || jj_done);
  }

 inline bool jj_2_1152(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1152() || jj_done);
  }

 inline bool jj_2_1153(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1153() || jj_done);
  }

 inline bool jj_2_1154(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1154() || jj_done);
  }

 inline bool jj_2_1155(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1155() || jj_done);
  }

 inline bool jj_2_1156(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1156() || jj_done);
  }

 inline bool jj_2_1157(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1157() || jj_done);
  }

 inline bool jj_2_1158(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1158() || jj_done);
  }

 inline bool jj_2_1159(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1159() || jj_done);
  }

 inline bool jj_2_1160(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1160() || jj_done);
  }

 inline bool jj_2_1161(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1161() || jj_done);
  }

 inline bool jj_2_1162(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1162() || jj_done);
  }

 inline bool jj_2_1163(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1163() || jj_done);
  }

 inline bool jj_2_1164(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1164() || jj_done);
  }

 inline bool jj_2_1165(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1165() || jj_done);
  }

 inline bool jj_2_1166(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1166() || jj_done);
  }

 inline bool jj_2_1167(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1167() || jj_done);
  }

 inline bool jj_2_1168(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1168() || jj_done);
  }

 inline bool jj_2_1169(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1169() || jj_done);
  }

 inline bool jj_2_1170(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1170() || jj_done);
  }

 inline bool jj_2_1171(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1171() || jj_done);
  }

 inline bool jj_2_1172(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1172() || jj_done);
  }

 inline bool jj_2_1173(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1173() || jj_done);
  }

 inline bool jj_2_1174(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1174() || jj_done);
  }

 inline bool jj_2_1175(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1175() || jj_done);
  }

 inline bool jj_2_1176(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1176() || jj_done);
  }

 inline bool jj_2_1177(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1177() || jj_done);
  }

 inline bool jj_2_1178(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1178() || jj_done);
  }

 inline bool jj_2_1179(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1179() || jj_done);
  }

 inline bool jj_2_1180(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1180() || jj_done);
  }

 inline bool jj_2_1181(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1181() || jj_done);
  }

 inline bool jj_2_1182(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1182() || jj_done);
  }

 inline bool jj_2_1183(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1183() || jj_done);
  }

 inline bool jj_2_1184(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1184() || jj_done);
  }

 inline bool jj_2_1185(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1185() || jj_done);
  }

 inline bool jj_2_1186(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1186() || jj_done);
  }

 inline bool jj_2_1187(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1187() || jj_done);
  }

 inline bool jj_2_1188(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1188() || jj_done);
  }

 inline bool jj_2_1189(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1189() || jj_done);
  }

 inline bool jj_2_1190(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1190() || jj_done);
  }

 inline bool jj_2_1191(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1191() || jj_done);
  }

 inline bool jj_2_1192(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1192() || jj_done);
  }

 inline bool jj_2_1193(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1193() || jj_done);
  }

 inline bool jj_2_1194(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1194() || jj_done);
  }

 inline bool jj_2_1195(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1195() || jj_done);
  }

 inline bool jj_2_1196(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1196() || jj_done);
  }

 inline bool jj_2_1197(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1197() || jj_done);
  }

 inline bool jj_2_1198(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1198() || jj_done);
  }

 inline bool jj_2_1199(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1199() || jj_done);
  }

 inline bool jj_2_1200(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1200() || jj_done);
  }

 inline bool jj_2_1201(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1201() || jj_done);
  }

 inline bool jj_2_1202(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1202() || jj_done);
  }

 inline bool jj_2_1203(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1203() || jj_done);
  }

 inline bool jj_2_1204(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1204() || jj_done);
  }

 inline bool jj_2_1205(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1205() || jj_done);
  }

 inline bool jj_2_1206(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1206() || jj_done);
  }

 inline bool jj_2_1207(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1207() || jj_done);
  }

 inline bool jj_2_1208(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1208() || jj_done);
  }

 inline bool jj_2_1209(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1209() || jj_done);
  }

 inline bool jj_2_1210(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1210() || jj_done);
  }

 inline bool jj_2_1211(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1211() || jj_done);
  }

 inline bool jj_2_1212(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1212() || jj_done);
  }

 inline bool jj_2_1213(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1213() || jj_done);
  }

 inline bool jj_2_1214(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1214() || jj_done);
  }

 inline bool jj_2_1215(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1215() || jj_done);
  }

 inline bool jj_2_1216(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1216() || jj_done);
  }

 inline bool jj_2_1217(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1217() || jj_done);
  }

 inline bool jj_2_1218(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1218() || jj_done);
  }

 inline bool jj_2_1219(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1219() || jj_done);
  }

 inline bool jj_2_1220(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1220() || jj_done);
  }

 inline bool jj_2_1221(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1221() || jj_done);
  }

 inline bool jj_2_1222(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1222() || jj_done);
  }

 inline bool jj_2_1223(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1223() || jj_done);
  }

 inline bool jj_2_1224(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1224() || jj_done);
  }

 inline bool jj_2_1225(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1225() || jj_done);
  }

 inline bool jj_2_1226(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1226() || jj_done);
  }

 inline bool jj_2_1227(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1227() || jj_done);
  }

 inline bool jj_2_1228(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1228() || jj_done);
  }

 inline bool jj_2_1229(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1229() || jj_done);
  }

 inline bool jj_2_1230(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1230() || jj_done);
  }

 inline bool jj_2_1231(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1231() || jj_done);
  }

 inline bool jj_2_1232(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1232() || jj_done);
  }

 inline bool jj_2_1233(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1233() || jj_done);
  }

 inline bool jj_2_1234(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1234() || jj_done);
  }

 inline bool jj_2_1235(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1235() || jj_done);
  }

 inline bool jj_2_1236(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1236() || jj_done);
  }

 inline bool jj_2_1237(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1237() || jj_done);
  }

 inline bool jj_2_1238(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1238() || jj_done);
  }

 inline bool jj_2_1239(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1239() || jj_done);
  }

 inline bool jj_2_1240(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1240() || jj_done);
  }

 inline bool jj_2_1241(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1241() || jj_done);
  }

 inline bool jj_2_1242(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1242() || jj_done);
  }

 inline bool jj_2_1243(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1243() || jj_done);
  }

 inline bool jj_2_1244(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1244() || jj_done);
  }

 inline bool jj_2_1245(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1245() || jj_done);
  }

 inline bool jj_2_1246(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1246() || jj_done);
  }

 inline bool jj_2_1247(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1247() || jj_done);
  }

 inline bool jj_2_1248(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1248() || jj_done);
  }

 inline bool jj_2_1249(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1249() || jj_done);
  }

 inline bool jj_2_1250(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1250() || jj_done);
  }

 inline bool jj_2_1251(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1251() || jj_done);
  }

 inline bool jj_2_1252(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1252() || jj_done);
  }

 inline bool jj_2_1253(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1253() || jj_done);
  }

 inline bool jj_2_1254(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1254() || jj_done);
  }

 inline bool jj_2_1255(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1255() || jj_done);
  }

 inline bool jj_2_1256(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1256() || jj_done);
  }

 inline bool jj_2_1257(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1257() || jj_done);
  }

 inline bool jj_2_1258(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1258() || jj_done);
  }

 inline bool jj_2_1259(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1259() || jj_done);
  }

 inline bool jj_2_1260(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1260() || jj_done);
  }

 inline bool jj_2_1261(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1261() || jj_done);
  }

 inline bool jj_2_1262(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1262() || jj_done);
  }

 inline bool jj_2_1263(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1263() || jj_done);
  }

 inline bool jj_2_1264(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1264() || jj_done);
  }

 inline bool jj_2_1265(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1265() || jj_done);
  }

 inline bool jj_2_1266(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1266() || jj_done);
  }

 inline bool jj_2_1267(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1267() || jj_done);
  }

 inline bool jj_2_1268(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1268() || jj_done);
  }

 inline bool jj_2_1269(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1269() || jj_done);
  }

 inline bool jj_2_1270(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1270() || jj_done);
  }

 inline bool jj_2_1271(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1271() || jj_done);
  }

 inline bool jj_2_1272(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1272() || jj_done);
  }

 inline bool jj_2_1273(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1273() || jj_done);
  }

 inline bool jj_2_1274(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1274() || jj_done);
  }

 inline bool jj_2_1275(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1275() || jj_done);
  }

 inline bool jj_2_1276(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1276() || jj_done);
  }

 inline bool jj_2_1277(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1277() || jj_done);
  }

 inline bool jj_2_1278(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1278() || jj_done);
  }

 inline bool jj_2_1279(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1279() || jj_done);
  }

 inline bool jj_2_1280(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1280() || jj_done);
  }

 inline bool jj_2_1281(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1281() || jj_done);
  }

 inline bool jj_2_1282(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1282() || jj_done);
  }

 inline bool jj_2_1283(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1283() || jj_done);
  }

 inline bool jj_2_1284(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1284() || jj_done);
  }

 inline bool jj_2_1285(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1285() || jj_done);
  }

 inline bool jj_2_1286(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1286() || jj_done);
  }

 inline bool jj_2_1287(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1287() || jj_done);
  }

 inline bool jj_2_1288(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1288() || jj_done);
  }

 inline bool jj_2_1289(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1289() || jj_done);
  }

 inline bool jj_2_1290(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1290() || jj_done);
  }

 inline bool jj_2_1291(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1291() || jj_done);
  }

 inline bool jj_2_1292(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1292() || jj_done);
  }

 inline bool jj_2_1293(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1293() || jj_done);
  }

 inline bool jj_2_1294(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1294() || jj_done);
  }

 inline bool jj_2_1295(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1295() || jj_done);
  }

 inline bool jj_2_1296(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1296() || jj_done);
  }

 inline bool jj_2_1297(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1297() || jj_done);
  }

 inline bool jj_2_1298(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1298() || jj_done);
  }

 inline bool jj_2_1299(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1299() || jj_done);
  }

 inline bool jj_2_1300(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1300() || jj_done);
  }

 inline bool jj_2_1301(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1301() || jj_done);
  }

 inline bool jj_2_1302(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1302() || jj_done);
  }

 inline bool jj_2_1303(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1303() || jj_done);
  }

 inline bool jj_2_1304(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1304() || jj_done);
  }

 inline bool jj_2_1305(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1305() || jj_done);
  }

 inline bool jj_2_1306(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1306() || jj_done);
  }

 inline bool jj_2_1307(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1307() || jj_done);
  }

 inline bool jj_2_1308(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1308() || jj_done);
  }

 inline bool jj_2_1309(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1309() || jj_done);
  }

 inline bool jj_2_1310(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1310() || jj_done);
  }

 inline bool jj_2_1311(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1311() || jj_done);
  }

 inline bool jj_2_1312(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1312() || jj_done);
  }

 inline bool jj_2_1313(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1313() || jj_done);
  }

 inline bool jj_2_1314(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1314() || jj_done);
  }

 inline bool jj_2_1315(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1315() || jj_done);
  }

 inline bool jj_2_1316(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1316() || jj_done);
  }

 inline bool jj_2_1317(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1317() || jj_done);
  }

 inline bool jj_2_1318(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1318() || jj_done);
  }

 inline bool jj_2_1319(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1319() || jj_done);
  }

 inline bool jj_2_1320(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1320() || jj_done);
  }

 inline bool jj_2_1321(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1321() || jj_done);
  }

 inline bool jj_2_1322(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1322() || jj_done);
  }

 inline bool jj_2_1323(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1323() || jj_done);
  }

 inline bool jj_2_1324(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1324() || jj_done);
  }

 inline bool jj_2_1325(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1325() || jj_done);
  }

 inline bool jj_2_1326(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1326() || jj_done);
  }

 inline bool jj_2_1327(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1327() || jj_done);
  }

 inline bool jj_2_1328(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1328() || jj_done);
  }

 inline bool jj_2_1329(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1329() || jj_done);
  }

 inline bool jj_2_1330(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1330() || jj_done);
  }

 inline bool jj_2_1331(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1331() || jj_done);
  }

 inline bool jj_2_1332(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1332() || jj_done);
  }

 inline bool jj_2_1333(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1333() || jj_done);
  }

 inline bool jj_2_1334(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1334() || jj_done);
  }

 inline bool jj_2_1335(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1335() || jj_done);
  }

 inline bool jj_2_1336(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1336() || jj_done);
  }

 inline bool jj_2_1337(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1337() || jj_done);
  }

 inline bool jj_2_1338(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1338() || jj_done);
  }

 inline bool jj_2_1339(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1339() || jj_done);
  }

 inline bool jj_2_1340(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1340() || jj_done);
  }

 inline bool jj_2_1341(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1341() || jj_done);
  }

 inline bool jj_2_1342(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1342() || jj_done);
  }

 inline bool jj_2_1343(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1343() || jj_done);
  }

 inline bool jj_2_1344(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1344() || jj_done);
  }

 inline bool jj_2_1345(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1345() || jj_done);
  }

 inline bool jj_2_1346(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1346() || jj_done);
  }

 inline bool jj_2_1347(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1347() || jj_done);
  }

 inline bool jj_2_1348(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1348() || jj_done);
  }

 inline bool jj_2_1349(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1349() || jj_done);
  }

 inline bool jj_2_1350(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1350() || jj_done);
  }

 inline bool jj_2_1351(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1351() || jj_done);
  }

 inline bool jj_2_1352(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1352() || jj_done);
  }

 inline bool jj_2_1353(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1353() || jj_done);
  }

 inline bool jj_2_1354(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1354() || jj_done);
  }

 inline bool jj_2_1355(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1355() || jj_done);
  }

 inline bool jj_2_1356(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1356() || jj_done);
  }

 inline bool jj_2_1357(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1357() || jj_done);
  }

 inline bool jj_2_1358(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1358() || jj_done);
  }

 inline bool jj_2_1359(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1359() || jj_done);
  }

 inline bool jj_2_1360(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1360() || jj_done);
  }

 inline bool jj_2_1361(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1361() || jj_done);
  }

 inline bool jj_2_1362(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1362() || jj_done);
  }

 inline bool jj_2_1363(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1363() || jj_done);
  }

 inline bool jj_2_1364(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1364() || jj_done);
  }

 inline bool jj_2_1365(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1365() || jj_done);
  }

 inline bool jj_2_1366(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1366() || jj_done);
  }

 inline bool jj_2_1367(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1367() || jj_done);
  }

 inline bool jj_2_1368(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1368() || jj_done);
  }

 inline bool jj_2_1369(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1369() || jj_done);
  }

 inline bool jj_2_1370(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1370() || jj_done);
  }

 inline bool jj_2_1371(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1371() || jj_done);
  }

 inline bool jj_2_1372(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1372() || jj_done);
  }

 inline bool jj_2_1373(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1373() || jj_done);
  }

 inline bool jj_2_1374(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1374() || jj_done);
  }

 inline bool jj_2_1375(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1375() || jj_done);
  }

 inline bool jj_2_1376(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1376() || jj_done);
  }

 inline bool jj_2_1377(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1377() || jj_done);
  }

 inline bool jj_2_1378(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1378() || jj_done);
  }

 inline bool jj_2_1379(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1379() || jj_done);
  }

 inline bool jj_2_1380(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1380() || jj_done);
  }

 inline bool jj_2_1381(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1381() || jj_done);
  }

 inline bool jj_2_1382(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1382() || jj_done);
  }

 inline bool jj_2_1383(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1383() || jj_done);
  }

 inline bool jj_2_1384(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1384() || jj_done);
  }

 inline bool jj_2_1385(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1385() || jj_done);
  }

 inline bool jj_2_1386(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1386() || jj_done);
  }

 inline bool jj_2_1387(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1387() || jj_done);
  }

 inline bool jj_2_1388(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1388() || jj_done);
  }

 inline bool jj_2_1389(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1389() || jj_done);
  }

 inline bool jj_2_1390(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1390() || jj_done);
  }

 inline bool jj_2_1391(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1391() || jj_done);
  }

 inline bool jj_2_1392(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1392() || jj_done);
  }

 inline bool jj_2_1393(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1393() || jj_done);
  }

 inline bool jj_2_1394(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1394() || jj_done);
  }

 inline bool jj_2_1395(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1395() || jj_done);
  }

 inline bool jj_2_1396(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1396() || jj_done);
  }

 inline bool jj_2_1397(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1397() || jj_done);
  }

 inline bool jj_2_1398(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1398() || jj_done);
  }

 inline bool jj_2_1399(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1399() || jj_done);
  }

 inline bool jj_2_1400(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1400() || jj_done);
  }

 inline bool jj_2_1401(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1401() || jj_done);
  }

 inline bool jj_2_1402(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1402() || jj_done);
  }

 inline bool jj_2_1403(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1403() || jj_done);
  }

 inline bool jj_2_1404(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1404() || jj_done);
  }

 inline bool jj_2_1405(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1405() || jj_done);
  }

 inline bool jj_2_1406(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1406() || jj_done);
  }

 inline bool jj_2_1407(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1407() || jj_done);
  }

 inline bool jj_2_1408(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1408() || jj_done);
  }

 inline bool jj_2_1409(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1409() || jj_done);
  }

 inline bool jj_2_1410(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1410() || jj_done);
  }

 inline bool jj_2_1411(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1411() || jj_done);
  }

 inline bool jj_2_1412(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1412() || jj_done);
  }

 inline bool jj_2_1413(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1413() || jj_done);
  }

 inline bool jj_2_1414(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1414() || jj_done);
  }

 inline bool jj_2_1415(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1415() || jj_done);
  }

 inline bool jj_2_1416(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1416() || jj_done);
  }

 inline bool jj_2_1417(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1417() || jj_done);
  }

 inline bool jj_2_1418(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1418() || jj_done);
  }

 inline bool jj_2_1419(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1419() || jj_done);
  }

 inline bool jj_2_1420(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1420() || jj_done);
  }

 inline bool jj_2_1421(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1421() || jj_done);
  }

 inline bool jj_2_1422(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1422() || jj_done);
  }

 inline bool jj_2_1423(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1423() || jj_done);
  }

 inline bool jj_2_1424(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1424() || jj_done);
  }

 inline bool jj_2_1425(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1425() || jj_done);
  }

 inline bool jj_2_1426(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1426() || jj_done);
  }

 inline bool jj_2_1427(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1427() || jj_done);
  }

 inline bool jj_2_1428(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1428() || jj_done);
  }

 inline bool jj_2_1429(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1429() || jj_done);
  }

 inline bool jj_2_1430(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1430() || jj_done);
  }

 inline bool jj_2_1431(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1431() || jj_done);
  }

 inline bool jj_2_1432(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1432() || jj_done);
  }

 inline bool jj_2_1433(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1433() || jj_done);
  }

 inline bool jj_2_1434(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1434() || jj_done);
  }

 inline bool jj_2_1435(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1435() || jj_done);
  }

 inline bool jj_2_1436(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1436() || jj_done);
  }

 inline bool jj_2_1437(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1437() || jj_done);
  }

 inline bool jj_2_1438(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1438() || jj_done);
  }

 inline bool jj_2_1439(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1439() || jj_done);
  }

 inline bool jj_2_1440(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1440() || jj_done);
  }

 inline bool jj_2_1441(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1441() || jj_done);
  }

 inline bool jj_2_1442(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1442() || jj_done);
  }

 inline bool jj_2_1443(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1443() || jj_done);
  }

 inline bool jj_2_1444(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1444() || jj_done);
  }

 inline bool jj_2_1445(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1445() || jj_done);
  }

 inline bool jj_2_1446(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1446() || jj_done);
  }

 inline bool jj_2_1447(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1447() || jj_done);
  }

 inline bool jj_2_1448(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1448() || jj_done);
  }

 inline bool jj_2_1449(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1449() || jj_done);
  }

 inline bool jj_2_1450(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1450() || jj_done);
  }

 inline bool jj_2_1451(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1451() || jj_done);
  }

 inline bool jj_2_1452(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1452() || jj_done);
  }

 inline bool jj_2_1453(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1453() || jj_done);
  }

 inline bool jj_2_1454(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1454() || jj_done);
  }

 inline bool jj_2_1455(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1455() || jj_done);
  }

 inline bool jj_2_1456(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1456() || jj_done);
  }

 inline bool jj_2_1457(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1457() || jj_done);
  }

 inline bool jj_2_1458(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1458() || jj_done);
  }

 inline bool jj_2_1459(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1459() || jj_done);
  }

 inline bool jj_2_1460(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1460() || jj_done);
  }

 inline bool jj_2_1461(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1461() || jj_done);
  }

 inline bool jj_2_1462(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1462() || jj_done);
  }

 inline bool jj_2_1463(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1463() || jj_done);
  }

 inline bool jj_2_1464(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1464() || jj_done);
  }

 inline bool jj_2_1465(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1465() || jj_done);
  }

 inline bool jj_2_1466(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1466() || jj_done);
  }

 inline bool jj_2_1467(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1467() || jj_done);
  }

 inline bool jj_2_1468(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1468() || jj_done);
  }

 inline bool jj_2_1469(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1469() || jj_done);
  }

 inline bool jj_2_1470(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1470() || jj_done);
  }

 inline bool jj_2_1471(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1471() || jj_done);
  }

 inline bool jj_2_1472(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1472() || jj_done);
  }

 inline bool jj_2_1473(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1473() || jj_done);
  }

 inline bool jj_2_1474(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1474() || jj_done);
  }

 inline bool jj_2_1475(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1475() || jj_done);
  }

 inline bool jj_2_1476(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1476() || jj_done);
  }

 inline bool jj_2_1477(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1477() || jj_done);
  }

 inline bool jj_2_1478(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1478() || jj_done);
  }

 inline bool jj_2_1479(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1479() || jj_done);
  }

 inline bool jj_2_1480(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1480() || jj_done);
  }

 inline bool jj_2_1481(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1481() || jj_done);
  }

 inline bool jj_2_1482(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1482() || jj_done);
  }

 inline bool jj_2_1483(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1483() || jj_done);
  }

 inline bool jj_2_1484(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1484() || jj_done);
  }

 inline bool jj_2_1485(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1485() || jj_done);
  }

 inline bool jj_2_1486(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1486() || jj_done);
  }

 inline bool jj_2_1487(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1487() || jj_done);
  }

 inline bool jj_2_1488(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1488() || jj_done);
  }

 inline bool jj_2_1489(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1489() || jj_done);
  }

 inline bool jj_2_1490(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1490() || jj_done);
  }

 inline bool jj_2_1491(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1491() || jj_done);
  }

 inline bool jj_2_1492(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1492() || jj_done);
  }

 inline bool jj_2_1493(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1493() || jj_done);
  }

 inline bool jj_2_1494(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1494() || jj_done);
  }

 inline bool jj_2_1495(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1495() || jj_done);
  }

 inline bool jj_2_1496(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1496() || jj_done);
  }

 inline bool jj_2_1497(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1497() || jj_done);
  }

 inline bool jj_2_1498(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1498() || jj_done);
  }

 inline bool jj_2_1499(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1499() || jj_done);
  }

 inline bool jj_2_1500(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1500() || jj_done);
  }

 inline bool jj_2_1501(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1501() || jj_done);
  }

 inline bool jj_2_1502(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1502() || jj_done);
  }

 inline bool jj_2_1503(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1503() || jj_done);
  }

 inline bool jj_2_1504(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1504() || jj_done);
  }

 inline bool jj_2_1505(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1505() || jj_done);
  }

 inline bool jj_2_1506(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1506() || jj_done);
  }

 inline bool jj_2_1507(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1507() || jj_done);
  }

 inline bool jj_2_1508(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1508() || jj_done);
  }

 inline bool jj_2_1509(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1509() || jj_done);
  }

 inline bool jj_2_1510(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1510() || jj_done);
  }

 inline bool jj_2_1511(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1511() || jj_done);
  }

 inline bool jj_2_1512(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1512() || jj_done);
  }

 inline bool jj_2_1513(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1513() || jj_done);
  }

 inline bool jj_2_1514(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1514() || jj_done);
  }

 inline bool jj_2_1515(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1515() || jj_done);
  }

 inline bool jj_2_1516(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1516() || jj_done);
  }

 inline bool jj_2_1517(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1517() || jj_done);
  }

 inline bool jj_2_1518(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1518() || jj_done);
  }

 inline bool jj_2_1519(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1519() || jj_done);
  }

 inline bool jj_2_1520(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1520() || jj_done);
  }

 inline bool jj_2_1521(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1521() || jj_done);
  }

 inline bool jj_2_1522(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1522() || jj_done);
  }

 inline bool jj_2_1523(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1523() || jj_done);
  }

 inline bool jj_2_1524(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1524() || jj_done);
  }

 inline bool jj_2_1525(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1525() || jj_done);
  }

 inline bool jj_2_1526(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1526() || jj_done);
  }

 inline bool jj_2_1527(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1527() || jj_done);
  }

 inline bool jj_2_1528(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1528() || jj_done);
  }

 inline bool jj_2_1529(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1529() || jj_done);
  }

 inline bool jj_2_1530(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1530() || jj_done);
  }

 inline bool jj_2_1531(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1531() || jj_done);
  }

 inline bool jj_2_1532(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1532() || jj_done);
  }

 inline bool jj_2_1533(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1533() || jj_done);
  }

 inline bool jj_2_1534(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1534() || jj_done);
  }

 inline bool jj_2_1535(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1535() || jj_done);
  }

 inline bool jj_2_1536(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1536() || jj_done);
  }

 inline bool jj_2_1537(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1537() || jj_done);
  }

 inline bool jj_2_1538(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1538() || jj_done);
  }

 inline bool jj_2_1539(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1539() || jj_done);
  }

 inline bool jj_2_1540(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1540() || jj_done);
  }

 inline bool jj_2_1541(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1541() || jj_done);
  }

 inline bool jj_2_1542(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1542() || jj_done);
  }

 inline bool jj_2_1543(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1543() || jj_done);
  }

 inline bool jj_2_1544(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1544() || jj_done);
  }

 inline bool jj_2_1545(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1545() || jj_done);
  }

 inline bool jj_2_1546(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1546() || jj_done);
  }

 inline bool jj_2_1547(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1547() || jj_done);
  }

 inline bool jj_2_1548(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1548() || jj_done);
  }

 inline bool jj_2_1549(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1549() || jj_done);
  }

 inline bool jj_2_1550(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1550() || jj_done);
  }

 inline bool jj_2_1551(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1551() || jj_done);
  }

 inline bool jj_2_1552(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1552() || jj_done);
  }

 inline bool jj_2_1553(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1553() || jj_done);
  }

 inline bool jj_2_1554(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1554() || jj_done);
  }

 inline bool jj_2_1555(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1555() || jj_done);
  }

 inline bool jj_2_1556(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1556() || jj_done);
  }

 inline bool jj_2_1557(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1557() || jj_done);
  }

 inline bool jj_2_1558(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1558() || jj_done);
  }

 inline bool jj_2_1559(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1559() || jj_done);
  }

 inline bool jj_2_1560(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1560() || jj_done);
  }

 inline bool jj_2_1561(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1561() || jj_done);
  }

 inline bool jj_2_1562(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1562() || jj_done);
  }

 inline bool jj_2_1563(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1563() || jj_done);
  }

 inline bool jj_2_1564(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1564() || jj_done);
  }

 inline bool jj_2_1565(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1565() || jj_done);
  }

 inline bool jj_2_1566(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1566() || jj_done);
  }

 inline bool jj_2_1567(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1567() || jj_done);
  }

 inline bool jj_2_1568(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1568() || jj_done);
  }

 inline bool jj_2_1569(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1569() || jj_done);
  }

 inline bool jj_2_1570(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1570() || jj_done);
  }

 inline bool jj_2_1571(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1571() || jj_done);
  }

 inline bool jj_2_1572(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1572() || jj_done);
  }

 inline bool jj_2_1573(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1573() || jj_done);
  }

 inline bool jj_2_1574(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1574() || jj_done);
  }

 inline bool jj_2_1575(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1575() || jj_done);
  }

 inline bool jj_2_1576(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1576() || jj_done);
  }

 inline bool jj_2_1577(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1577() || jj_done);
  }

 inline bool jj_2_1578(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1578() || jj_done);
  }

 inline bool jj_2_1579(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1579() || jj_done);
  }

 inline bool jj_2_1580(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1580() || jj_done);
  }

 inline bool jj_2_1581(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1581() || jj_done);
  }

 inline bool jj_2_1582(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1582() || jj_done);
  }

 inline bool jj_2_1583(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1583() || jj_done);
  }

 inline bool jj_2_1584(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1584() || jj_done);
  }

 inline bool jj_2_1585(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1585() || jj_done);
  }

 inline bool jj_2_1586(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1586() || jj_done);
  }

 inline bool jj_2_1587(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1587() || jj_done);
  }

 inline bool jj_2_1588(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1588() || jj_done);
  }

 inline bool jj_2_1589(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1589() || jj_done);
  }

 inline bool jj_2_1590(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1590() || jj_done);
  }

 inline bool jj_2_1591(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1591() || jj_done);
  }

 inline bool jj_2_1592(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1592() || jj_done);
  }

 inline bool jj_2_1593(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1593() || jj_done);
  }

 inline bool jj_2_1594(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1594() || jj_done);
  }

 inline bool jj_2_1595(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1595() || jj_done);
  }

 inline bool jj_2_1596(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1596() || jj_done);
  }

 inline bool jj_2_1597(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1597() || jj_done);
  }

 inline bool jj_2_1598(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1598() || jj_done);
  }

 inline bool jj_2_1599(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1599() || jj_done);
  }

 inline bool jj_2_1600(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1600() || jj_done);
  }

 inline bool jj_2_1601(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1601() || jj_done);
  }

 inline bool jj_2_1602(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1602() || jj_done);
  }

 inline bool jj_2_1603(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1603() || jj_done);
  }

 inline bool jj_2_1604(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1604() || jj_done);
  }

 inline bool jj_2_1605(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1605() || jj_done);
  }

 inline bool jj_2_1606(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1606() || jj_done);
  }

 inline bool jj_2_1607(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1607() || jj_done);
  }

 inline bool jj_2_1608(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1608() || jj_done);
  }

 inline bool jj_2_1609(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1609() || jj_done);
  }

 inline bool jj_2_1610(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1610() || jj_done);
  }

 inline bool jj_2_1611(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1611() || jj_done);
  }

 inline bool jj_2_1612(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1612() || jj_done);
  }

 inline bool jj_2_1613(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1613() || jj_done);
  }

 inline bool jj_2_1614(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1614() || jj_done);
  }

 inline bool jj_2_1615(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1615() || jj_done);
  }

 inline bool jj_2_1616(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1616() || jj_done);
  }

 inline bool jj_2_1617(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1617() || jj_done);
  }

 inline bool jj_2_1618(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1618() || jj_done);
  }

 inline bool jj_2_1619(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1619() || jj_done);
  }

 inline bool jj_2_1620(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1620() || jj_done);
  }

 inline bool jj_2_1621(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1621() || jj_done);
  }

 inline bool jj_2_1622(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1622() || jj_done);
  }

 inline bool jj_2_1623(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1623() || jj_done);
  }

 inline bool jj_2_1624(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1624() || jj_done);
  }

 inline bool jj_2_1625(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1625() || jj_done);
  }

 inline bool jj_2_1626(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1626() || jj_done);
  }

 inline bool jj_2_1627(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1627() || jj_done);
  }

 inline bool jj_2_1628(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1628() || jj_done);
  }

 inline bool jj_2_1629(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1629() || jj_done);
  }

 inline bool jj_2_1630(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1630() || jj_done);
  }

 inline bool jj_2_1631(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1631() || jj_done);
  }

 inline bool jj_2_1632(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1632() || jj_done);
  }

 inline bool jj_2_1633(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1633() || jj_done);
  }

 inline bool jj_2_1634(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1634() || jj_done);
  }

 inline bool jj_2_1635(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1635() || jj_done);
  }

 inline bool jj_2_1636(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1636() || jj_done);
  }

 inline bool jj_2_1637(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1637() || jj_done);
  }

 inline bool jj_2_1638(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1638() || jj_done);
  }

 inline bool jj_2_1639(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1639() || jj_done);
  }

 inline bool jj_2_1640(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1640() || jj_done);
  }

 inline bool jj_2_1641(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1641() || jj_done);
  }

 inline bool jj_2_1642(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1642() || jj_done);
  }

 inline bool jj_2_1643(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1643() || jj_done);
  }

 inline bool jj_2_1644(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1644() || jj_done);
  }

 inline bool jj_2_1645(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1645() || jj_done);
  }

 inline bool jj_2_1646(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1646() || jj_done);
  }

 inline bool jj_2_1647(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1647() || jj_done);
  }

 inline bool jj_2_1648(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1648() || jj_done);
  }

 inline bool jj_2_1649(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1649() || jj_done);
  }

 inline bool jj_2_1650(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1650() || jj_done);
  }

 inline bool jj_2_1651(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1651() || jj_done);
  }

 inline bool jj_2_1652(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1652() || jj_done);
  }

 inline bool jj_2_1653(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1653() || jj_done);
  }

 inline bool jj_2_1654(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1654() || jj_done);
  }

 inline bool jj_2_1655(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1655() || jj_done);
  }

 inline bool jj_2_1656(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1656() || jj_done);
  }

 inline bool jj_2_1657(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1657() || jj_done);
  }

 inline bool jj_2_1658(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1658() || jj_done);
  }

 inline bool jj_2_1659(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1659() || jj_done);
  }

 inline bool jj_2_1660(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1660() || jj_done);
  }

 inline bool jj_2_1661(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1661() || jj_done);
  }

 inline bool jj_2_1662(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1662() || jj_done);
  }

 inline bool jj_2_1663(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1663() || jj_done);
  }

 inline bool jj_2_1664(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1664() || jj_done);
  }

 inline bool jj_2_1665(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1665() || jj_done);
  }

 inline bool jj_2_1666(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1666() || jj_done);
  }

 inline bool jj_2_1667(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1667() || jj_done);
  }

 inline bool jj_2_1668(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1668() || jj_done);
  }

 inline bool jj_2_1669(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1669() || jj_done);
  }

 inline bool jj_2_1670(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1670() || jj_done);
  }

 inline bool jj_2_1671(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1671() || jj_done);
  }

 inline bool jj_2_1672(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1672() || jj_done);
  }

 inline bool jj_2_1673(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1673() || jj_done);
  }

 inline bool jj_2_1674(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1674() || jj_done);
  }

 inline bool jj_2_1675(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1675() || jj_done);
  }

 inline bool jj_2_1676(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1676() || jj_done);
  }

 inline bool jj_2_1677(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1677() || jj_done);
  }

 inline bool jj_2_1678(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1678() || jj_done);
  }

 inline bool jj_2_1679(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1679() || jj_done);
  }

 inline bool jj_2_1680(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1680() || jj_done);
  }

 inline bool jj_2_1681(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1681() || jj_done);
  }

 inline bool jj_2_1682(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1682() || jj_done);
  }

 inline bool jj_2_1683(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1683() || jj_done);
  }

 inline bool jj_2_1684(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1684() || jj_done);
  }

 inline bool jj_2_1685(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1685() || jj_done);
  }

 inline bool jj_2_1686(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1686() || jj_done);
  }

 inline bool jj_2_1687(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1687() || jj_done);
  }

 inline bool jj_2_1688(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1688() || jj_done);
  }

 inline bool jj_2_1689(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1689() || jj_done);
  }

 inline bool jj_2_1690(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1690() || jj_done);
  }

 inline bool jj_2_1691(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1691() || jj_done);
  }

 inline bool jj_2_1692(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1692() || jj_done);
  }

 inline bool jj_2_1693(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1693() || jj_done);
  }

 inline bool jj_2_1694(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1694() || jj_done);
  }

 inline bool jj_2_1695(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1695() || jj_done);
  }

 inline bool jj_2_1696(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1696() || jj_done);
  }

 inline bool jj_2_1697(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1697() || jj_done);
  }

 inline bool jj_2_1698(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1698() || jj_done);
  }

 inline bool jj_2_1699(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1699() || jj_done);
  }

 inline bool jj_2_1700(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1700() || jj_done);
  }

 inline bool jj_2_1701(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1701() || jj_done);
  }

 inline bool jj_2_1702(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1702() || jj_done);
  }

 inline bool jj_2_1703(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1703() || jj_done);
  }

 inline bool jj_2_1704(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1704() || jj_done);
  }

 inline bool jj_2_1705(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1705() || jj_done);
  }

 inline bool jj_2_1706(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1706() || jj_done);
  }

 inline bool jj_2_1707(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1707() || jj_done);
  }

 inline bool jj_2_1708(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1708() || jj_done);
  }

 inline bool jj_2_1709(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1709() || jj_done);
  }

 inline bool jj_2_1710(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1710() || jj_done);
  }

 inline bool jj_2_1711(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1711() || jj_done);
  }

 inline bool jj_2_1712(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1712() || jj_done);
  }

 inline bool jj_2_1713(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1713() || jj_done);
  }

 inline bool jj_2_1714(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1714() || jj_done);
  }

 inline bool jj_2_1715(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1715() || jj_done);
  }

 inline bool jj_2_1716(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1716() || jj_done);
  }

 inline bool jj_2_1717(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1717() || jj_done);
  }

 inline bool jj_2_1718(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1718() || jj_done);
  }

 inline bool jj_2_1719(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1719() || jj_done);
  }

 inline bool jj_2_1720(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1720() || jj_done);
  }

 inline bool jj_2_1721(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1721() || jj_done);
  }

 inline bool jj_2_1722(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1722() || jj_done);
  }

 inline bool jj_2_1723(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1723() || jj_done);
  }

 inline bool jj_2_1724(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1724() || jj_done);
  }

 inline bool jj_2_1725(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1725() || jj_done);
  }

 inline bool jj_2_1726(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1726() || jj_done);
  }

 inline bool jj_2_1727(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1727() || jj_done);
  }

 inline bool jj_2_1728(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1728() || jj_done);
  }

 inline bool jj_2_1729(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1729() || jj_done);
  }

 inline bool jj_2_1730(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1730() || jj_done);
  }

 inline bool jj_2_1731(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1731() || jj_done);
  }

 inline bool jj_2_1732(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1732() || jj_done);
  }

 inline bool jj_2_1733(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1733() || jj_done);
  }

 inline bool jj_2_1734(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1734() || jj_done);
  }

 inline bool jj_2_1735(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1735() || jj_done);
  }

 inline bool jj_2_1736(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1736() || jj_done);
  }

 inline bool jj_2_1737(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1737() || jj_done);
  }

 inline bool jj_2_1738(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1738() || jj_done);
  }

 inline bool jj_2_1739(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1739() || jj_done);
  }

 inline bool jj_2_1740(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1740() || jj_done);
  }

 inline bool jj_2_1741(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1741() || jj_done);
  }

 inline bool jj_2_1742(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1742() || jj_done);
  }

 inline bool jj_2_1743(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1743() || jj_done);
  }

 inline bool jj_2_1744(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1744() || jj_done);
  }

 inline bool jj_2_1745(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1745() || jj_done);
  }

 inline bool jj_2_1746(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1746() || jj_done);
  }

 inline bool jj_2_1747(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1747() || jj_done);
  }

 inline bool jj_2_1748(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1748() || jj_done);
  }

 inline bool jj_2_1749(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1749() || jj_done);
  }

 inline bool jj_2_1750(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1750() || jj_done);
  }

 inline bool jj_2_1751(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1751() || jj_done);
  }

 inline bool jj_2_1752(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1752() || jj_done);
  }

 inline bool jj_2_1753(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1753() || jj_done);
  }

 inline bool jj_2_1754(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1754() || jj_done);
  }

 inline bool jj_2_1755(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1755() || jj_done);
  }

 inline bool jj_2_1756(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1756() || jj_done);
  }

 inline bool jj_2_1757(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1757() || jj_done);
  }

 inline bool jj_2_1758(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1758() || jj_done);
  }

 inline bool jj_2_1759(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1759() || jj_done);
  }

 inline bool jj_2_1760(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1760() || jj_done);
  }

 inline bool jj_2_1761(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1761() || jj_done);
  }

 inline bool jj_2_1762(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1762() || jj_done);
  }

 inline bool jj_2_1763(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1763() || jj_done);
  }

 inline bool jj_2_1764(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1764() || jj_done);
  }

 inline bool jj_2_1765(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1765() || jj_done);
  }

 inline bool jj_2_1766(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1766() || jj_done);
  }

 inline bool jj_2_1767(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1767() || jj_done);
  }

 inline bool jj_2_1768(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1768() || jj_done);
  }

 inline bool jj_2_1769(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1769() || jj_done);
  }

 inline bool jj_2_1770(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1770() || jj_done);
  }

 inline bool jj_2_1771(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1771() || jj_done);
  }

 inline bool jj_2_1772(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1772() || jj_done);
  }

 inline bool jj_2_1773(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1773() || jj_done);
  }

 inline bool jj_2_1774(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1774() || jj_done);
  }

 inline bool jj_2_1775(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1775() || jj_done);
  }

 inline bool jj_2_1776(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1776() || jj_done);
  }

 inline bool jj_2_1777(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1777() || jj_done);
  }

 inline bool jj_2_1778(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1778() || jj_done);
  }

 inline bool jj_2_1779(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1779() || jj_done);
  }

 inline bool jj_2_1780(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1780() || jj_done);
  }

 inline bool jj_2_1781(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1781() || jj_done);
  }

 inline bool jj_2_1782(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1782() || jj_done);
  }

 inline bool jj_2_1783(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1783() || jj_done);
  }

 inline bool jj_2_1784(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1784() || jj_done);
  }

 inline bool jj_2_1785(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1785() || jj_done);
  }

 inline bool jj_2_1786(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1786() || jj_done);
  }

 inline bool jj_2_1787(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1787() || jj_done);
  }

 inline bool jj_2_1788(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1788() || jj_done);
  }

 inline bool jj_2_1789(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1789() || jj_done);
  }

 inline bool jj_2_1790(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1790() || jj_done);
  }

 inline bool jj_2_1791(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1791() || jj_done);
  }

 inline bool jj_2_1792(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1792() || jj_done);
  }

 inline bool jj_2_1793(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1793() || jj_done);
  }

 inline bool jj_2_1794(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1794() || jj_done);
  }

 inline bool jj_2_1795(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1795() || jj_done);
  }

 inline bool jj_2_1796(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1796() || jj_done);
  }

 inline bool jj_2_1797(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1797() || jj_done);
  }

 inline bool jj_2_1798(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1798() || jj_done);
  }

 inline bool jj_2_1799(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1799() || jj_done);
  }

 inline bool jj_2_1800(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1800() || jj_done);
  }

 inline bool jj_2_1801(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1801() || jj_done);
  }

 inline bool jj_2_1802(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1802() || jj_done);
  }

 inline bool jj_2_1803(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1803() || jj_done);
  }

 inline bool jj_2_1804(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1804() || jj_done);
  }

 inline bool jj_2_1805(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1805() || jj_done);
  }

 inline bool jj_2_1806(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1806() || jj_done);
  }

 inline bool jj_2_1807(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1807() || jj_done);
  }

 inline bool jj_2_1808(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1808() || jj_done);
  }

 inline bool jj_2_1809(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1809() || jj_done);
  }

 inline bool jj_2_1810(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1810() || jj_done);
  }

 inline bool jj_2_1811(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1811() || jj_done);
  }

 inline bool jj_2_1812(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1812() || jj_done);
  }

 inline bool jj_2_1813(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1813() || jj_done);
  }

 inline bool jj_2_1814(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1814() || jj_done);
  }

 inline bool jj_2_1815(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1815() || jj_done);
  }

 inline bool jj_2_1816(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1816() || jj_done);
  }

 inline bool jj_2_1817(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1817() || jj_done);
  }

 inline bool jj_2_1818(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1818() || jj_done);
  }

 inline bool jj_2_1819(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1819() || jj_done);
  }

 inline bool jj_2_1820(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1820() || jj_done);
  }

 inline bool jj_2_1821(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1821() || jj_done);
  }

 inline bool jj_2_1822(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1822() || jj_done);
  }

 inline bool jj_2_1823(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1823() || jj_done);
  }

 inline bool jj_2_1824(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1824() || jj_done);
  }

 inline bool jj_2_1825(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1825() || jj_done);
  }

 inline bool jj_2_1826(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1826() || jj_done);
  }

 inline bool jj_2_1827(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1827() || jj_done);
  }

 inline bool jj_2_1828(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1828() || jj_done);
  }

 inline bool jj_2_1829(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1829() || jj_done);
  }

 inline bool jj_2_1830(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1830() || jj_done);
  }

 inline bool jj_2_1831(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1831() || jj_done);
  }

 inline bool jj_2_1832(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1832() || jj_done);
  }

 inline bool jj_2_1833(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1833() || jj_done);
  }

 inline bool jj_2_1834(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1834() || jj_done);
  }

 inline bool jj_2_1835(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1835() || jj_done);
  }

 inline bool jj_2_1836(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1836() || jj_done);
  }

 inline bool jj_2_1837(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1837() || jj_done);
  }

 inline bool jj_2_1838(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1838() || jj_done);
  }

 inline bool jj_2_1839(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1839() || jj_done);
  }

 inline bool jj_2_1840(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1840() || jj_done);
  }

 inline bool jj_2_1841(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1841() || jj_done);
  }

 inline bool jj_2_1842(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1842() || jj_done);
  }

 inline bool jj_2_1843(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1843() || jj_done);
  }

 inline bool jj_2_1844(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1844() || jj_done);
  }

 inline bool jj_2_1845(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1845() || jj_done);
  }

 inline bool jj_2_1846(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1846() || jj_done);
  }

 inline bool jj_2_1847(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1847() || jj_done);
  }

 inline bool jj_2_1848(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1848() || jj_done);
  }

 inline bool jj_2_1849(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1849() || jj_done);
  }

 inline bool jj_2_1850(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1850() || jj_done);
  }

 inline bool jj_2_1851(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1851() || jj_done);
  }

 inline bool jj_2_1852(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1852() || jj_done);
  }

 inline bool jj_2_1853(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1853() || jj_done);
  }

 inline bool jj_2_1854(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1854() || jj_done);
  }

 inline bool jj_2_1855(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1855() || jj_done);
  }

 inline bool jj_2_1856(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1856() || jj_done);
  }

 inline bool jj_2_1857(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1857() || jj_done);
  }

 inline bool jj_2_1858(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1858() || jj_done);
  }

 inline bool jj_2_1859(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1859() || jj_done);
  }

 inline bool jj_2_1860(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1860() || jj_done);
  }

 inline bool jj_2_1861(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1861() || jj_done);
  }

 inline bool jj_2_1862(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1862() || jj_done);
  }

 inline bool jj_2_1863(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1863() || jj_done);
  }

 inline bool jj_2_1864(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1864() || jj_done);
  }

 inline bool jj_2_1865(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1865() || jj_done);
  }

 inline bool jj_2_1866(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1866() || jj_done);
  }

 inline bool jj_2_1867(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1867() || jj_done);
  }

 inline bool jj_2_1868(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1868() || jj_done);
  }

 inline bool jj_2_1869(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1869() || jj_done);
  }

 inline bool jj_2_1870(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1870() || jj_done);
  }

 inline bool jj_2_1871(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1871() || jj_done);
  }

 inline bool jj_2_1872(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1872() || jj_done);
  }

 inline bool jj_2_1873(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1873() || jj_done);
  }

 inline bool jj_2_1874(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1874() || jj_done);
  }

 inline bool jj_2_1875(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1875() || jj_done);
  }

 inline bool jj_2_1876(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1876() || jj_done);
  }

 inline bool jj_2_1877(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1877() || jj_done);
  }

 inline bool jj_2_1878(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1878() || jj_done);
  }

 inline bool jj_2_1879(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1879() || jj_done);
  }

 inline bool jj_2_1880(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1880() || jj_done);
  }

 inline bool jj_2_1881(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1881() || jj_done);
  }

 inline bool jj_2_1882(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1882() || jj_done);
  }

 inline bool jj_2_1883(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1883() || jj_done);
  }

 inline bool jj_2_1884(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1884() || jj_done);
  }

 inline bool jj_2_1885(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1885() || jj_done);
  }

 inline bool jj_2_1886(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1886() || jj_done);
  }

 inline bool jj_2_1887(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1887() || jj_done);
  }

 inline bool jj_2_1888(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1888() || jj_done);
  }

 inline bool jj_2_1889(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1889() || jj_done);
  }

 inline bool jj_2_1890(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1890() || jj_done);
  }

 inline bool jj_2_1891(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1891() || jj_done);
  }

 inline bool jj_2_1892(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1892() || jj_done);
  }

 inline bool jj_2_1893(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1893() || jj_done);
  }

 inline bool jj_2_1894(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1894() || jj_done);
  }

 inline bool jj_2_1895(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1895() || jj_done);
  }

 inline bool jj_2_1896(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1896() || jj_done);
  }

 inline bool jj_2_1897(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1897() || jj_done);
  }

 inline bool jj_2_1898(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1898() || jj_done);
  }

 inline bool jj_2_1899(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1899() || jj_done);
  }

 inline bool jj_2_1900(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1900() || jj_done);
  }

 inline bool jj_2_1901(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1901() || jj_done);
  }

 inline bool jj_2_1902(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1902() || jj_done);
  }

 inline bool jj_2_1903(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1903() || jj_done);
  }

 inline bool jj_2_1904(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1904() || jj_done);
  }

 inline bool jj_2_1905(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1905() || jj_done);
  }

 inline bool jj_2_1906(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1906() || jj_done);
  }

 inline bool jj_2_1907(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1907() || jj_done);
  }

 inline bool jj_2_1908(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1908() || jj_done);
  }

 inline bool jj_2_1909(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1909() || jj_done);
  }

 inline bool jj_2_1910(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1910() || jj_done);
  }

 inline bool jj_2_1911(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1911() || jj_done);
  }

 inline bool jj_2_1912(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1912() || jj_done);
  }

 inline bool jj_2_1913(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1913() || jj_done);
  }

 inline bool jj_2_1914(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1914() || jj_done);
  }

 inline bool jj_2_1915(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1915() || jj_done);
  }

 inline bool jj_2_1916(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1916() || jj_done);
  }

 inline bool jj_2_1917(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1917() || jj_done);
  }

 inline bool jj_2_1918(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1918() || jj_done);
  }

 inline bool jj_2_1919(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1919() || jj_done);
  }

 inline bool jj_2_1920(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1920() || jj_done);
  }

 inline bool jj_2_1921(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1921() || jj_done);
  }

 inline bool jj_2_1922(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1922() || jj_done);
  }

 inline bool jj_2_1923(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1923() || jj_done);
  }

 inline bool jj_2_1924(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1924() || jj_done);
  }

 inline bool jj_2_1925(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1925() || jj_done);
  }

 inline bool jj_2_1926(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1926() || jj_done);
  }

 inline bool jj_2_1927(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1927() || jj_done);
  }

 inline bool jj_2_1928(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1928() || jj_done);
  }

 inline bool jj_2_1929(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1929() || jj_done);
  }

 inline bool jj_2_1930(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1930() || jj_done);
  }

 inline bool jj_2_1931(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1931() || jj_done);
  }

 inline bool jj_2_1932(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1932() || jj_done);
  }

 inline bool jj_2_1933(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1933() || jj_done);
  }

 inline bool jj_2_1934(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1934() || jj_done);
  }

 inline bool jj_2_1935(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1935() || jj_done);
  }

 inline bool jj_2_1936(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1936() || jj_done);
  }

 inline bool jj_2_1937(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1937() || jj_done);
  }

 inline bool jj_2_1938(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1938() || jj_done);
  }

 inline bool jj_2_1939(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1939() || jj_done);
  }

 inline bool jj_2_1940(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1940() || jj_done);
  }

 inline bool jj_2_1941(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1941() || jj_done);
  }

 inline bool jj_2_1942(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1942() || jj_done);
  }

 inline bool jj_2_1943(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1943() || jj_done);
  }

 inline bool jj_2_1944(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1944() || jj_done);
  }

 inline bool jj_2_1945(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1945() || jj_done);
  }

 inline bool jj_2_1946(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1946() || jj_done);
  }

 inline bool jj_2_1947(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1947() || jj_done);
  }

 inline bool jj_2_1948(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1948() || jj_done);
  }

 inline bool jj_2_1949(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1949() || jj_done);
  }

 inline bool jj_2_1950(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1950() || jj_done);
  }

 inline bool jj_2_1951(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1951() || jj_done);
  }

 inline bool jj_2_1952(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1952() || jj_done);
  }

 inline bool jj_2_1953(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1953() || jj_done);
  }

 inline bool jj_2_1954(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1954() || jj_done);
  }

 inline bool jj_2_1955(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1955() || jj_done);
  }

 inline bool jj_2_1956(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1956() || jj_done);
  }

 inline bool jj_2_1957(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1957() || jj_done);
  }

 inline bool jj_2_1958(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1958() || jj_done);
  }

 inline bool jj_2_1959(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1959() || jj_done);
  }

 inline bool jj_2_1960(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1960() || jj_done);
  }

 inline bool jj_2_1961(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1961() || jj_done);
  }

 inline bool jj_2_1962(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1962() || jj_done);
  }

 inline bool jj_2_1963(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1963() || jj_done);
  }

 inline bool jj_2_1964(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1964() || jj_done);
  }

 inline bool jj_2_1965(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1965() || jj_done);
  }

 inline bool jj_2_1966(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1966() || jj_done);
  }

 inline bool jj_2_1967(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1967() || jj_done);
  }

 inline bool jj_2_1968(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1968() || jj_done);
  }

 inline bool jj_2_1969(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1969() || jj_done);
  }

 inline bool jj_2_1970(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1970() || jj_done);
  }

 inline bool jj_2_1971(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1971() || jj_done);
  }

 inline bool jj_2_1972(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1972() || jj_done);
  }

 inline bool jj_2_1973(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1973() || jj_done);
  }

 inline bool jj_2_1974(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1974() || jj_done);
  }

 inline bool jj_2_1975(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1975() || jj_done);
  }

 inline bool jj_2_1976(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1976() || jj_done);
  }

 inline bool jj_2_1977(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1977() || jj_done);
  }

 inline bool jj_2_1978(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1978() || jj_done);
  }

 inline bool jj_2_1979(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1979() || jj_done);
  }

 inline bool jj_2_1980(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1980() || jj_done);
  }

 inline bool jj_2_1981(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1981() || jj_done);
  }

 inline bool jj_2_1982(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1982() || jj_done);
  }

 inline bool jj_2_1983(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1983() || jj_done);
  }

 inline bool jj_2_1984(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1984() || jj_done);
  }

 inline bool jj_2_1985(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1985() || jj_done);
  }

 inline bool jj_2_1986(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1986() || jj_done);
  }

 inline bool jj_2_1987(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1987() || jj_done);
  }

 inline bool jj_2_1988(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1988() || jj_done);
  }

 inline bool jj_2_1989(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1989() || jj_done);
  }

 inline bool jj_2_1990(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1990() || jj_done);
  }

 inline bool jj_2_1991(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1991() || jj_done);
  }

 inline bool jj_2_1992(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1992() || jj_done);
  }

 inline bool jj_2_1993(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1993() || jj_done);
  }

 inline bool jj_2_1994(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1994() || jj_done);
  }

 inline bool jj_2_1995(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1995() || jj_done);
  }

 inline bool jj_2_1996(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1996() || jj_done);
  }

 inline bool jj_2_1997(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1997() || jj_done);
  }

 inline bool jj_2_1998(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1998() || jj_done);
  }

 inline bool jj_2_1999(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_1999() || jj_done);
  }

 inline bool jj_2_2000(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_2000() || jj_done);
  }

 inline bool jj_2_2001(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_2001() || jj_done);
  }

 inline bool jj_2_2002(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_2002() || jj_done);
  }

 inline bool jj_2_2003(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_2003() || jj_done);
  }

 inline bool jj_2_2004(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_2004() || jj_done);
  }

 inline bool jj_2_2005(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_2005() || jj_done);
  }

 inline bool jj_2_2006(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_2006() || jj_done);
  }

 inline bool jj_2_2007(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_2007() || jj_done);
  }

 inline bool jj_2_2008(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_2008() || jj_done);
  }

 inline bool jj_2_2009(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_2009() || jj_done);
  }

 inline bool jj_2_2010(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_2010() || jj_done);
  }

 inline bool jj_2_2011(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_2011() || jj_done);
  }

 inline bool jj_2_2012(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_2012() || jj_done);
  }

 inline bool jj_2_2013(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_2013() || jj_done);
  }

 inline bool jj_2_2014(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_2014() || jj_done);
  }

 inline bool jj_2_2015(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_2015() || jj_done);
  }

 inline bool jj_2_2016(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_2016() || jj_done);
  }

 inline bool jj_2_2017(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_2017() || jj_done);
  }

 inline bool jj_2_2018(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_2018() || jj_done);
  }

 inline bool jj_2_2019(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_2019() || jj_done);
  }

 inline bool jj_2_2020(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_2020() || jj_done);
  }

 inline bool jj_2_2021(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_2021() || jj_done);
  }

 inline bool jj_2_2022(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_2022() || jj_done);
  }

 inline bool jj_2_2023(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_2023() || jj_done);
  }

 inline bool jj_2_2024(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_2024() || jj_done);
  }

 inline bool jj_2_2025(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_2025() || jj_done);
  }

 inline bool jj_2_2026(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_2026() || jj_done);
  }

 inline bool jj_2_2027(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_2027() || jj_done);
  }

 inline bool jj_2_2028(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_2028() || jj_done);
  }

 inline bool jj_2_2029(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_2029() || jj_done);
  }

 inline bool jj_2_2030(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_2030() || jj_done);
  }

 inline bool jj_2_2031(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_2031() || jj_done);
  }

 inline bool jj_2_2032(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_2032() || jj_done);
  }

 inline bool jj_2_2033(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_2033() || jj_done);
  }

 inline bool jj_2_2034(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_2034() || jj_done);
  }

 inline bool jj_2_2035(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_2035() || jj_done);
  }

 inline bool jj_2_2036(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_2036() || jj_done);
  }

 inline bool jj_2_2037(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_2037() || jj_done);
  }

 inline bool jj_2_2038(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_2038() || jj_done);
  }

 inline bool jj_2_2039(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_2039() || jj_done);
  }

 inline bool jj_2_2040(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_2040() || jj_done);
  }

 inline bool jj_2_2041(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_2041() || jj_done);
  }

 inline bool jj_2_2042(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_2042() || jj_done);
  }

 inline bool jj_2_2043(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_2043() || jj_done);
  }

 inline bool jj_2_2044(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_2044() || jj_done);
  }

 inline bool jj_2_2045(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_2045() || jj_done);
  }

 inline bool jj_2_2046(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_2046() || jj_done);
  }

 inline bool jj_2_2047(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_2047() || jj_done);
  }

 inline bool jj_2_2048(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_2048() || jj_done);
  }

 inline bool jj_2_2049(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_2049() || jj_done);
  }

 inline bool jj_2_2050(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_2050() || jj_done);
  }

 inline bool jj_2_2051(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_2051() || jj_done);
  }

 inline bool jj_2_2052(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_2052() || jj_done);
  }

 inline bool jj_2_2053(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_2053() || jj_done);
  }

 inline bool jj_2_2054(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_2054() || jj_done);
  }

 inline bool jj_2_2055(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_2055() || jj_done);
  }

 inline bool jj_2_2056(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_2056() || jj_done);
  }

 inline bool jj_2_2057(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_2057() || jj_done);
  }

 inline bool jj_2_2058(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_2058() || jj_done);
  }

 inline bool jj_2_2059(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_2059() || jj_done);
  }

 inline bool jj_2_2060(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_2060() || jj_done);
  }

 inline bool jj_2_2061(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_2061() || jj_done);
  }

 inline bool jj_2_2062(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_2062() || jj_done);
  }

 inline bool jj_2_2063(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_2063() || jj_done);
  }

 inline bool jj_2_2064(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_2064() || jj_done);
  }

 inline bool jj_2_2065(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_2065() || jj_done);
  }

 inline bool jj_2_2066(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_2066() || jj_done);
  }

 inline bool jj_2_2067(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_2067() || jj_done);
  }

 inline bool jj_2_2068(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_2068() || jj_done);
  }

 inline bool jj_2_2069(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_2069() || jj_done);
  }

 inline bool jj_2_2070(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_2070() || jj_done);
  }

 inline bool jj_2_2071(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_2071() || jj_done);
  }

 inline bool jj_2_2072(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_2072() || jj_done);
  }

 inline bool jj_2_2073(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_2073() || jj_done);
  }

 inline bool jj_2_2074(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_2074() || jj_done);
  }

 inline bool jj_2_2075(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_2075() || jj_done);
  }

 inline bool jj_2_2076(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_2076() || jj_done);
  }

 inline bool jj_2_2077(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_2077() || jj_done);
  }

 inline bool jj_2_2078(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_2078() || jj_done);
  }

 inline bool jj_2_2079(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_2079() || jj_done);
  }

 inline bool jj_2_2080(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_2080() || jj_done);
  }

 inline bool jj_2_2081(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_2081() || jj_done);
  }

 inline bool jj_2_2082(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_2082() || jj_done);
  }

 inline bool jj_2_2083(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_2083() || jj_done);
  }

 inline bool jj_2_2084(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_2084() || jj_done);
  }

 inline bool jj_2_2085(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_2085() || jj_done);
  }

 inline bool jj_2_2086(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_2086() || jj_done);
  }

 inline bool jj_2_2087(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_2087() || jj_done);
  }

 inline bool jj_2_2088(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_2088() || jj_done);
  }

 inline bool jj_2_2089(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_2089() || jj_done);
  }

 inline bool jj_2_2090(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_2090() || jj_done);
  }

 inline bool jj_2_2091(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_2091() || jj_done);
  }

 inline bool jj_2_2092(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_2092() || jj_done);
  }

 inline bool jj_2_2093(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_2093() || jj_done);
  }

 inline bool jj_2_2094(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_2094() || jj_done);
  }

 inline bool jj_2_2095(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_2095() || jj_done);
  }

 inline bool jj_2_2096(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_2096() || jj_done);
  }

 inline bool jj_2_2097(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_2097() || jj_done);
  }

 inline bool jj_2_2098(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_2098() || jj_done);
  }

 inline bool jj_2_2099(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_2099() || jj_done);
  }

 inline bool jj_2_2100(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_2100() || jj_done);
  }

 inline bool jj_2_2101(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_2101() || jj_done);
  }

 inline bool jj_2_2102(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_2102() || jj_done);
  }

 inline bool jj_2_2103(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_2103() || jj_done);
  }

 inline bool jj_2_2104(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_2104() || jj_done);
  }

 inline bool jj_2_2105(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_2105() || jj_done);
  }

 inline bool jj_2_2106(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_2106() || jj_done);
  }

 inline bool jj_2_2107(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_2107() || jj_done);
  }

 inline bool jj_2_2108(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_2108() || jj_done);
  }

 inline bool jj_2_2109(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_2109() || jj_done);
  }

 inline bool jj_2_2110(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_2110() || jj_done);
  }

 inline bool jj_2_2111(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_2111() || jj_done);
  }

 inline bool jj_2_2112(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_2112() || jj_done);
  }

 inline bool jj_2_2113(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_2113() || jj_done);
  }

 inline bool jj_2_2114(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_2114() || jj_done);
  }

 inline bool jj_2_2115(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_2115() || jj_done);
  }

 inline bool jj_2_2116(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_2116() || jj_done);
  }

 inline bool jj_2_2117(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_2117() || jj_done);
  }

 inline bool jj_2_2118(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_2118() || jj_done);
  }

 inline bool jj_2_2119(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_2119() || jj_done);
  }

 inline bool jj_2_2120(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_2120() || jj_done);
  }

 inline bool jj_2_2121(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_2121() || jj_done);
  }

 inline bool jj_2_2122(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_2122() || jj_done);
  }

 inline bool jj_2_2123(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_2123() || jj_done);
  }

 inline bool jj_2_2124(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_2124() || jj_done);
  }

 inline bool jj_2_2125(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_2125() || jj_done);
  }

 inline bool jj_2_2126(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_2126() || jj_done);
  }

 inline bool jj_2_2127(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_2127() || jj_done);
  }

 inline bool jj_2_2128(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_2128() || jj_done);
  }

 inline bool jj_2_2129(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_2129() || jj_done);
  }

 inline bool jj_2_2130(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_2130() || jj_done);
  }

 inline bool jj_2_2131(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_2131() || jj_done);
  }

 inline bool jj_2_2132(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_2132() || jj_done);
  }

 inline bool jj_2_2133(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_2133() || jj_done);
  }

 inline bool jj_2_2134(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_2134() || jj_done);
  }

 inline bool jj_2_2135(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_2135() || jj_done);
  }

 inline bool jj_2_2136(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_2136() || jj_done);
  }

 inline bool jj_2_2137(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_2137() || jj_done);
  }

 inline bool jj_2_2138(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_2138() || jj_done);
  }

 inline bool jj_2_2139(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_2139() || jj_done);
  }

 inline bool jj_2_2140(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_2140() || jj_done);
  }

 inline bool jj_2_2141(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_2141() || jj_done);
  }

 inline bool jj_2_2142(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_2142() || jj_done);
  }

 inline bool jj_2_2143(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_2143() || jj_done);
  }

 inline bool jj_2_2144(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_2144() || jj_done);
  }

 inline bool jj_2_2145(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_2145() || jj_done);
  }

 inline bool jj_2_2146(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_2146() || jj_done);
  }

 inline bool jj_2_2147(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_2147() || jj_done);
  }

 inline bool jj_2_2148(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_2148() || jj_done);
  }

 inline bool jj_2_2149(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_2149() || jj_done);
  }

 inline bool jj_2_2150(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_2150() || jj_done);
  }

 inline bool jj_2_2151(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_2151() || jj_done);
  }

 inline bool jj_2_2152(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_2152() || jj_done);
  }

 inline bool jj_2_2153(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_2153() || jj_done);
  }

 inline bool jj_2_2154(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_2154() || jj_done);
  }

 inline bool jj_2_2155(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_2155() || jj_done);
  }

 inline bool jj_2_2156(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_2156() || jj_done);
  }

 inline bool jj_2_2157(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_2157() || jj_done);
  }

 inline bool jj_2_2158(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_2158() || jj_done);
  }

 inline bool jj_2_2159(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_2159() || jj_done);
  }

 inline bool jj_2_2160(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_2160() || jj_done);
  }

 inline bool jj_2_2161(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_2161() || jj_done);
  }

 inline bool jj_2_2162(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_2162() || jj_done);
  }

 inline bool jj_2_2163(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_2163() || jj_done);
  }

 inline bool jj_2_2164(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_2164() || jj_done);
  }

 inline bool jj_2_2165(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_2165() || jj_done);
  }

 inline bool jj_2_2166(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_2166() || jj_done);
  }

 inline bool jj_2_2167(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_2167() || jj_done);
  }

 inline bool jj_2_2168(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_2168() || jj_done);
  }

 inline bool jj_2_2169(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_2169() || jj_done);
  }

 inline bool jj_2_2170(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_2170() || jj_done);
  }

 inline bool jj_2_2171(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_2171() || jj_done);
  }

 inline bool jj_2_2172(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_2172() || jj_done);
  }

 inline bool jj_2_2173(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_2173() || jj_done);
  }

 inline bool jj_2_2174(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_2174() || jj_done);
  }

 inline bool jj_2_2175(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_2175() || jj_done);
  }

 inline bool jj_2_2176(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_2176() || jj_done);
  }

 inline bool jj_2_2177(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_2177() || jj_done);
  }

 inline bool jj_2_2178(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_2178() || jj_done);
  }

 inline bool jj_2_2179(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_2179() || jj_done);
  }

 inline bool jj_2_2180(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_2180() || jj_done);
  }

 inline bool jj_2_2181(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_2181() || jj_done);
  }

 inline bool jj_2_2182(int xla)
 {
    jj_la = xla; jj_lastpos = jj_scanpos = token;
    jj_done = false;
    return (!jj_3_2182() || jj_done);
  }

 inline bool jj_3R_sequence_generator_option_6137_5_691()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1654()) {
    jj_scanpos = xsp;
    if (jj_3_1655()) return true;
    }
    return false;
  }

 inline bool jj_3_1654()
 {
    if (jj_done) return true;
    if (jj_3R_sequence_generator_data_type_option_6166_5_692()) return true;
    return false;
  }

 inline bool jj_3_1653()
 {
    if (jj_done) return true;
    if (jj_3R_sequence_generator_option_6137_5_691()) return true;
    return false;
  }

 inline bool jj_3R_sequence_generator_options_6131_5_690()
 {
    if (jj_done) return true;
    Token * xsp;
    if (jj_3_1653()) return true;
    while (true) {
      xsp = jj_scanpos;
      if (jj_3_1653()) { jj_scanpos = xsp; break; }
    }
    return false;
  }

 inline bool jj_3_1649()
 {
    if (jj_done) return true;
    if (jj_scan_token(TRANSFORMS)) return true;
    return false;
  }

 inline bool jj_3R_sequence_generator_definition_6125_5_517()
 {
    if (jj_done) return true;
    if (jj_scan_token(CREATE)) return true;
    if (jj_scan_token(SEQUENCE)) return true;
    if (jj_3R_schema_qualified_name_970_5_252()) return true;
    return false;
  }

 inline bool jj_3R_transform_group_element_6119_5_689()
 {
    if (jj_done) return true;
    if (jj_3R_identifier_928_3_141()) return true;
    return false;
  }

 inline bool jj_3_1648()
 {
    if (jj_done) return true;
    if (jj_scan_token(TRANSFORM)) return true;
    return false;
  }

 inline bool jj_3_1651()
 {
    if (jj_done) return true;
    if (jj_3R_transform_group_element_6119_5_689()) return true;
    return false;
  }

 inline bool jj_3R_transforms_to_be_dropped_6112_5_993()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1650()) {
    jj_scanpos = xsp;
    if (jj_3_1651()) return true;
    }
    return false;
  }

 inline bool jj_3_1650()
 {
    if (jj_done) return true;
    if (jj_scan_token(ALL)) return true;
    return false;
  }

 inline bool jj_3_1641()
 {
    if (jj_done) return true;
    if (jj_3R_alter_group_6066_5_684()) return true;
    return false;
  }

 inline bool jj_3R_drop_transform_statement_6105_5_761()
 {
    if (jj_done) return true;
    if (jj_scan_token(DROP)) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1648()) {
    jj_scanpos = xsp;
    if (jj_3_1649()) return true;
    }
    if (jj_3R_transforms_to_be_dropped_6112_5_993()) return true;
    return false;
  }

 inline bool jj_3_1642()
 {
    if (jj_done) return true;
    if (jj_scan_token(570)) return true;
    if (jj_3R_alter_transform_action_6078_5_685()) return true;
    return false;
  }

 inline bool jj_3_1647()
 {
    if (jj_done) return true;
    if (jj_scan_token(FROM)) return true;
    if (jj_scan_token(SQL)) return true;
    return false;
  }

 inline bool jj_3R_transform_kind_6098_5_688()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1646()) {
    jj_scanpos = xsp;
    if (jj_3_1647()) return true;
    }
    return false;
  }

 inline bool jj_3_1646()
 {
    if (jj_done) return true;
    if (jj_scan_token(TO)) return true;
    if (jj_scan_token(SQL)) return true;
    return false;
  }

 inline bool jj_3_1645()
 {
    if (jj_done) return true;
    if (jj_scan_token(570)) return true;
    if (jj_3R_transform_kind_6098_5_688()) return true;
    return false;
  }

 inline bool jj_3R_drop_transform_element_list_6091_5_687()
 {
    if (jj_done) return true;
    if (jj_scan_token(DROP)) return true;
    if (jj_scan_token(lparen)) return true;
    if (jj_3R_transform_kind_6098_5_688()) return true;
    return false;
  }

 inline bool jj_3R_add_transform_element_list_6085_5_686()
 {
    if (jj_done) return true;
    if (jj_scan_token(ADD)) return true;
    if (jj_scan_token(lparen)) return true;
    if (jj_3R_transform_element_list_6022_5_984()) return true;
    return false;
  }

 inline bool jj_3_1640()
 {
    if (jj_done) return true;
    if (jj_scan_token(TRANSFORMS)) return true;
    return false;
  }

 inline bool jj_3_1644()
 {
    if (jj_done) return true;
    if (jj_3R_drop_transform_element_list_6091_5_687()) return true;
    return false;
  }

 inline bool jj_3R_alter_transform_action_6078_5_685()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1643()) {
    jj_scanpos = xsp;
    if (jj_3_1644()) return true;
    }
    return false;
  }

 inline bool jj_3_1643()
 {
    if (jj_done) return true;
    if (jj_3R_add_transform_element_list_6085_5_686()) return true;
    return false;
  }

 inline bool jj_3R_alter_transform_action_list_6072_5_985()
 {
    if (jj_done) return true;
    if (jj_3R_alter_transform_action_6078_5_685()) return true;
    return false;
  }

 inline bool jj_3_1639()
 {
    if (jj_done) return true;
    if (jj_scan_token(TRANSFORM)) return true;
    return false;
  }

 inline bool jj_3R_alter_group_6066_5_684()
 {
    if (jj_done) return true;
    if (jj_3R_identifier_928_3_141()) return true;
    if (jj_scan_token(lparen)) return true;
    if (jj_3R_alter_transform_action_list_6072_5_985()) return true;
    return false;
  }

 inline bool jj_3R_alter_transform_statement_6059_5_760()
 {
    if (jj_done) return true;
    if (jj_scan_token(ALTER)) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1639()) {
    jj_scanpos = xsp;
    if (jj_3_1640()) return true;
    }
    if (jj_scan_token(FOR)) return true;
    return false;
  }

 inline bool jj_3_1635()
 {
    if (jj_done) return true;
    if (jj_3R_transform_group_6016_5_680()) return true;
    return false;
  }

 inline bool jj_3_1636()
 {
    if (jj_done) return true;
    if (jj_scan_token(570)) return true;
    if (jj_3R_transform_element_6028_5_681()) return true;
    return false;
  }

 inline bool jj_3R_from_sql_6041_5_683()
 {
    if (jj_done) return true;
    if (jj_scan_token(FROM)) return true;
    if (jj_scan_token(SQL)) return true;
    if (jj_scan_token(WITH)) return true;
    return false;
  }

 inline bool jj_3R_to_sql_6035_5_682()
 {
    if (jj_done) return true;
    if (jj_scan_token(TO)) return true;
    if (jj_scan_token(SQL)) return true;
    if (jj_scan_token(WITH)) return true;
    return false;
  }

 inline bool jj_3_1634()
 {
    if (jj_done) return true;
    if (jj_scan_token(TRANSFORMS)) return true;
    return false;
  }

 inline bool jj_3_1638()
 {
    if (jj_done) return true;
    if (jj_3R_from_sql_6041_5_683()) return true;
    return false;
  }

 inline bool jj_3R_transform_element_6028_5_681()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1637()) {
    jj_scanpos = xsp;
    if (jj_3_1638()) return true;
    }
    return false;
  }

 inline bool jj_3_1637()
 {
    if (jj_done) return true;
    if (jj_3R_to_sql_6035_5_682()) return true;
    return false;
  }

 inline bool jj_3R_transform_element_list_6022_5_984()
 {
    if (jj_done) return true;
    if (jj_3R_transform_element_6028_5_681()) return true;
    return false;
  }

 inline bool jj_3_1633()
 {
    if (jj_done) return true;
    if (jj_scan_token(TRANSFORM)) return true;
    return false;
  }

 inline bool jj_3R_transform_group_6016_5_680()
 {
    if (jj_done) return true;
    if (jj_3R_identifier_928_3_141()) return true;
    if (jj_scan_token(lparen)) return true;
    if (jj_3R_transform_element_list_6022_5_984()) return true;
    return false;
  }

 inline bool jj_3R_transform_definition_6009_5_515()
 {
    if (jj_done) return true;
    if (jj_scan_token(CREATE)) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1633()) {
    jj_scanpos = xsp;
    if (jj_3_1634()) return true;
    }
    if (jj_scan_token(FOR)) return true;
    return false;
  }

 inline bool jj_3R_drop_user_defined_ordering_statement_6003_5_759()
 {
    if (jj_done) return true;
    if (jj_scan_token(DROP)) return true;
    if (jj_scan_token(ORDERING)) return true;
    if (jj_scan_token(FOR)) return true;
    return false;
  }

 inline bool jj_3R_map_function_specification_5997_5_983()
 {
    if (jj_done) return true;
    if (jj_3R_specific_routine_designator_4041_5_708()) return true;
    return false;
  }

 inline bool jj_3_1632()
 {
    if (jj_done) return true;
    if (jj_3R_schema_qualified_name_970_5_252()) return true;
    return false;
  }

 inline bool jj_3R_relative_function_specification_5991_5_982()
 {
    if (jj_done) return true;
    if (jj_3R_specific_routine_designator_4041_5_708()) return true;
    return false;
  }

 inline bool jj_3R_state_category_5985_5_679()
 {
    if (jj_done) return true;
    if (jj_scan_token(STATE)) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1632()) jj_scanpos = xsp;
    return false;
  }

 inline bool jj_3R_map_category_5979_5_678()
 {
    if (jj_done) return true;
    if (jj_scan_token(MAP)) return true;
    if (jj_scan_token(WITH)) return true;
    if (jj_3R_map_function_specification_5997_5_983()) return true;
    return false;
  }

 inline bool jj_3R_relative_category_5973_5_677()
 {
    if (jj_done) return true;
    if (jj_scan_token(RELATIVE)) return true;
    if (jj_scan_token(WITH)) return true;
    if (jj_3R_relative_function_specification_5991_5_982()) return true;
    return false;
  }

 inline bool jj_3_1631()
 {
    if (jj_done) return true;
    if (jj_3R_state_category_5985_5_679()) return true;
    return false;
  }

 inline bool jj_3_1630()
 {
    if (jj_done) return true;
    if (jj_3R_map_category_5979_5_678()) return true;
    return false;
  }

 inline bool jj_3_1629()
 {
    if (jj_done) return true;
    if (jj_3R_relative_category_5973_5_677()) return true;
    return false;
  }

 inline bool jj_3R_full_ordering_form_5959_5_676()
 {
    if (jj_done) return true;
    if (jj_scan_token(ORDER)) return true;
    if (jj_scan_token(FULL)) return true;
    if (jj_scan_token(BY)) return true;
    return false;
  }

 inline bool jj_3R_equals_ordering_form_5953_5_675()
 {
    if (jj_done) return true;
    if (jj_scan_token(EQUALS)) return true;
    if (jj_scan_token(ONLY)) return true;
    if (jj_scan_token(BY)) return true;
    return false;
  }

 inline bool jj_3_1628()
 {
    if (jj_done) return true;
    if (jj_3R_full_ordering_form_5959_5_676()) return true;
    return false;
  }

 inline bool jj_3_1627()
 {
    if (jj_done) return true;
    if (jj_3R_equals_ordering_form_5953_5_675()) return true;
    return false;
  }

 inline bool jj_3R_user_defined_ordering_definition_5940_5_514()
 {
    if (jj_done) return true;
    if (jj_scan_token(CREATE)) return true;
    if (jj_scan_token(ORDERING)) return true;
    if (jj_scan_token(FOR)) return true;
    return false;
  }

 inline bool jj_3R_drop_user_defined_cast_statement_5933_5_747()
 {
    if (jj_done) return true;
    if (jj_scan_token(DROP)) return true;
    if (jj_scan_token(CAST)) return true;
    if (jj_scan_token(lparen)) return true;
    return false;
  }

 inline bool jj_3_1626()
 {
    if (jj_done) return true;
    if (jj_scan_token(AS)) return true;
    if (jj_scan_token(ASSIGNMENT)) return true;
    return false;
  }

 inline bool jj_3R_user_defined_cast_definition_5907_5_513()
 {
    if (jj_done) return true;
    if (jj_scan_token(CREATE)) return true;
    if (jj_scan_token(CAST)) return true;
    if (jj_scan_token(lparen)) return true;
    return false;
  }

 inline bool jj_3R_drop_routine_statement_5901_5_746()
 {
    if (jj_done) return true;
    if (jj_scan_token(DROP)) return true;
    if (jj_3R_specific_routine_designator_4041_5_708()) return true;
    return false;
  }

 inline bool jj_3_1617()
 {
    if (jj_done) return true;
    if (jj_3R_multiple_group_specification_5859_5_672()) return true;
    return false;
  }

 inline bool jj_3_1625()
 {
    if (jj_done) return true;
    if (jj_scan_token(NAME)) return true;
    if (jj_3R_external_routine_name_1013_5_668()) return true;
    return false;
  }

 inline bool jj_3_1624()
 {
    if (jj_done) return true;
    if (jj_3R_returned_result_sets_characteristic_5700_5_659()) return true;
    return false;
  }

 inline bool jj_3_1623()
 {
    if (jj_done) return true;
    if (jj_3R_null_call_clause_5834_5_639()) return true;
    return false;
  }

 inline bool jj_3_1622()
 {
    if (jj_done) return true;
    if (jj_3R_SQL_data_access_indication_5825_5_638()) return true;
    return false;
  }

 inline bool jj_3_1621()
 {
    if (jj_done) return true;
    if (jj_3R_parameter_style_clause_5706_5_636()) return true;
    return false;
  }

 inline bool jj_3R_alter_routine_characteristic_5884_5_674()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1620()) {
    jj_scanpos = xsp;
    if (jj_3_1621()) {
    jj_scanpos = xsp;
    if (jj_3_1622()) {
    jj_scanpos = xsp;
    if (jj_3_1623()) {
    jj_scanpos = xsp;
    if (jj_3_1624()) {
    jj_scanpos = xsp;
    if (jj_3_1625()) return true;
    }
    }
    }
    }
    }
    return false;
  }

 inline bool jj_3_1620()
 {
    if (jj_done) return true;
    if (jj_3R_language_clause_3935_5_635()) return true;
    return false;
  }

 inline bool jj_3_1618()
 {
    if (jj_done) return true;
    if (jj_scan_token(570)) return true;
    if (jj_3R_group_specification_5865_5_673()) return true;
    return false;
  }

 inline bool jj_3_1619()
 {
    if (jj_done) return true;
    if (jj_3R_alter_routine_characteristic_5884_5_674()) return true;
    return false;
  }

 inline bool jj_3R_alter_routine_statement_5871_5_745()
 {
    if (jj_done) return true;
    if (jj_scan_token(ALTER)) return true;
    if (jj_3R_specific_routine_designator_4041_5_708()) return true;
    return false;
  }

 inline bool jj_3_1616()
 {
    if (jj_done) return true;
    if (jj_3R_single_group_specification_5853_5_671()) return true;
    return false;
  }

 inline bool jj_3R_group_specification_5865_5_673()
 {
    if (jj_done) return true;
    if (jj_3R_identifier_928_3_141()) return true;
    if (jj_scan_token(FOR)) return true;
    if (jj_scan_token(TYPE)) return true;
    return false;
  }

 inline bool jj_3R_multiple_group_specification_5859_5_672()
 {
    if (jj_done) return true;
    if (jj_3R_group_specification_5865_5_673()) return true;
    return false;
  }

 inline bool jj_3R_single_group_specification_5853_5_671()
 {
    if (jj_done) return true;
    if (jj_3R_identifier_928_3_141()) return true;
    return false;
  }

 inline bool jj_3R_transform_group_specification_5847_5_669()
 {
    if (jj_done) return true;
    if (jj_scan_token(TRANSFORM)) return true;
    if (jj_scan_token(GROUP)) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1616()) {
    jj_scanpos = xsp;
    if (jj_3_1617()) return true;
    }
    return false;
  }

 inline bool jj_3_1615()
 {
    if (jj_done) return true;
    if (jj_scan_token(CALLED)) return true;
    if (jj_scan_token(ON)) return true;
    if (jj_scan_token(NULL_)) return true;
    return false;
  }

 inline bool jj_3R_null_call_clause_5834_5_639()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1614()) {
    jj_scanpos = xsp;
    if (jj_3_1615()) return true;
    }
    return false;
  }

 inline bool jj_3_1614()
 {
    if (jj_done) return true;
    if (jj_scan_token(RETURNS)) return true;
    if (jj_scan_token(NULL_)) return true;
    if (jj_scan_token(ON)) return true;
    return false;
  }

 inline bool jj_3_1613()
 {
    if (jj_done) return true;
    if (jj_scan_token(MODIFIES)) return true;
    if (jj_scan_token(SQL)) return true;
    if (jj_scan_token(DATA)) return true;
    return false;
  }

 inline bool jj_3_1612()
 {
    if (jj_done) return true;
    if (jj_scan_token(READS)) return true;
    if (jj_scan_token(SQL)) return true;
    if (jj_scan_token(DATA)) return true;
    return false;
  }

 inline bool jj_3_1611()
 {
    if (jj_done) return true;
    if (jj_scan_token(CONTAINS)) return true;
    if (jj_scan_token(SQL)) return true;
    return false;
  }

 inline bool jj_3R_SQL_data_access_indication_5825_5_638()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1610()) {
    jj_scanpos = xsp;
    if (jj_3_1611()) {
    jj_scanpos = xsp;
    if (jj_3_1612()) {
    jj_scanpos = xsp;
    if (jj_3_1613()) return true;
    }
    }
    }
    return false;
  }

 inline bool jj_3_1610()
 {
    if (jj_done) return true;
    if (jj_scan_token(NO)) return true;
    if (jj_scan_token(SQL)) return true;
    return false;
  }

 inline bool jj_3_1609()
 {
    if (jj_done) return true;
    if (jj_scan_token(NOT)) return true;
    if (jj_scan_token(DETERMINISTIC)) return true;
    return false;
  }

 inline bool jj_3R_deterministic_characteristic_5818_5_637()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1608()) {
    jj_scanpos = xsp;
    if (jj_3_1609()) return true;
    }
    return false;
  }

 inline bool jj_3_1608()
 {
    if (jj_done) return true;
    if (jj_scan_token(DETERMINISTIC)) return true;
    return false;
  }

 inline bool jj_3_1607()
 {
    if (jj_done) return true;
    if (jj_scan_token(GENERAL)) return true;
    return false;
  }

 inline bool jj_3R_parameter_style_5811_5_973()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1606()) {
    jj_scanpos = xsp;
    if (jj_3_1607()) return true;
    }
    return false;
  }

 inline bool jj_3_1606()
 {
    if (jj_done) return true;
    if (jj_scan_token(SQL)) return true;
    return false;
  }

 inline bool jj_3_1599()
 {
    if (jj_done) return true;
    if (jj_scan_token(NAME)) return true;
    if (jj_3R_external_routine_name_1013_5_668()) return true;
    return false;
  }

 inline bool jj_3_1605()
 {
    if (jj_done) return true;
    if (jj_scan_token(EXTERNAL)) return true;
    if (jj_scan_token(SECURITY)) return true;
    if (jj_scan_token(IMPLEMENTATION)) return true;
    return false;
  }

 inline bool jj_3_1604()
 {
    if (jj_done) return true;
    if (jj_scan_token(EXTERNAL)) return true;
    if (jj_scan_token(SECURITY)) return true;
    if (jj_scan_token(INVOKER)) return true;
    return false;
  }

 inline bool jj_3R_external_security_clause_5803_5_670()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1603()) {
    jj_scanpos = xsp;
    if (jj_3_1604()) {
    jj_scanpos = xsp;
    if (jj_3_1605()) return true;
    }
    }
    return false;
  }

 inline bool jj_3_1603()
 {
    if (jj_done) return true;
    if (jj_scan_token(EXTERNAL)) return true;
    if (jj_scan_token(SECURITY)) return true;
    if (jj_scan_token(DEFINER)) return true;
    return false;
  }

 inline bool jj_3_1602()
 {
    if (jj_done) return true;
    if (jj_3R_external_security_clause_5803_5_670()) return true;
    return false;
  }

 inline bool jj_3_1601()
 {
    if (jj_done) return true;
    if (jj_3R_transform_group_specification_5847_5_669()) return true;
    return false;
  }

 inline bool jj_3_1600()
 {
    if (jj_done) return true;
    if (jj_3R_parameter_style_clause_5706_5_636()) return true;
    return false;
  }

 inline bool jj_3R_external_body_reference_5794_5_666()
 {
    if (jj_done) return true;
    if (jj_scan_token(EXTERNAL)) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1599()) jj_scanpos = xsp;
    xsp = jj_scanpos;
    if (jj_3_1600()) jj_scanpos = xsp;
    xsp = jj_scanpos;
    if (jj_3_1601()) jj_scanpos = xsp;
    xsp = jj_scanpos;
    if (jj_3_1602()) jj_scanpos = xsp;
    return false;
  }

 inline bool jj_3R_SQL_routine_body_5788_5_981()
 {
    if (jj_done) return true;
    if (jj_3R_SQL_procedure_statement_6507_5_610()) return true;
    return false;
  }

 inline bool jj_3_1598()
 {
    if (jj_done) return true;
    if (jj_scan_token(SQL)) return true;
    if (jj_scan_token(SECURITY)) return true;
    if (jj_scan_token(DEFINER)) return true;
    return false;
  }

 inline bool jj_3R_rights_clause_5781_5_667()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1597()) {
    jj_scanpos = xsp;
    if (jj_3_1598()) return true;
    }
    return false;
  }

 inline bool jj_3_1597()
 {
    if (jj_done) return true;
    if (jj_scan_token(SQL)) return true;
    if (jj_scan_token(SECURITY)) return true;
    if (jj_scan_token(INVOKER)) return true;
    return false;
  }

 inline bool jj_3_1596()
 {
    if (jj_done) return true;
    if (jj_3R_rights_clause_5781_5_667()) return true;
    return false;
  }

 inline bool jj_3_1593()
 {
    if (jj_done) return true;
    if (jj_3R_locator_indication_5648_5_653()) return true;
    return false;
  }

 inline bool jj_3R_SQL_routine_spec_5775_5_665()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1596()) jj_scanpos = xsp;
    if (jj_3R_SQL_routine_body_5788_5_981()) return true;
    return false;
  }

 inline bool jj_3_1592()
 {
    if (jj_done) return true;
    if (jj_3R_locator_indication_5648_5_653()) return true;
    return false;
  }

 inline bool jj_3_1595()
 {
    if (jj_done) return true;
    if (jj_3R_external_body_reference_5794_5_666()) return true;
    return false;
  }

 inline bool jj_3_1594()
 {
    if (jj_done) return true;
    if (jj_3R_SQL_routine_spec_5775_5_665()) return true;
    return false;
  }

 inline bool jj_3R_returns_data_type_5762_5_662()
 {
    if (jj_done) return true;
    if (jj_3R_data_type_1086_3_251()) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1593()) jj_scanpos = xsp;
    return false;
  }

 inline bool jj_3R_result_cast_from_type_5756_5_979()
 {
    if (jj_done) return true;
    if (jj_3R_data_type_1086_3_251()) return true;
    return false;
  }

 inline bool jj_3R_result_cast_5750_5_661()
 {
    if (jj_done) return true;
    if (jj_scan_token(CAST)) return true;
    if (jj_scan_token(FROM)) return true;
    if (jj_3R_result_cast_from_type_5756_5_979()) return true;
    return false;
  }

 inline bool jj_3_1588()
 {
    if (jj_done) return true;
    if (jj_3R_result_cast_5750_5_661()) return true;
    return false;
  }

 inline bool jj_3R_table_function_column_list_element_5744_5_664()
 {
    if (jj_done) return true;
    if (jj_3R_identifier_928_3_141()) return true;
    if (jj_3R_data_type_1086_3_251()) return true;
    return false;
  }

 inline bool jj_3_1591()
 {
    if (jj_done) return true;
    if (jj_scan_token(570)) return true;
    if (jj_3R_table_function_column_list_element_5744_5_664()) return true;
    return false;
  }

 inline bool jj_3R_table_function_column_list_5737_6_980()
 {
    if (jj_done) return true;
    if (jj_scan_token(lparen)) return true;
    if (jj_3R_table_function_column_list_element_5744_5_664()) return true;
    return false;
  }

 inline bool jj_3R_returns_table_type_5731_5_663()
 {
    if (jj_done) return true;
    if (jj_scan_token(TABLE)) return true;
    if (jj_3R_table_function_column_list_5737_6_980()) return true;
    return false;
  }

 inline bool jj_3_1590()
 {
    if (jj_done) return true;
    if (jj_3R_returns_table_type_5731_5_663()) return true;
    return false;
  }

 inline bool jj_3R_returns_type_5724_5_978()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1589()) {
    jj_scanpos = xsp;
    if (jj_3_1590()) return true;
    }
    return false;
  }

 inline bool jj_3_1589()
 {
    if (jj_done) return true;
    if (jj_3R_returns_data_type_5762_5_662()) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1588()) jj_scanpos = xsp;
    return false;
  }

 inline bool jj_3R_returns_clause_5718_5_656()
 {
    if (jj_done) return true;
    if (jj_scan_token(RETURNS)) return true;
    if (jj_3R_returns_type_5724_5_978()) return true;
    return false;
  }

 inline bool jj_3R_dispatch_clause_5712_5_655()
 {
    if (jj_done) return true;
    if (jj_scan_token(STATIC)) return true;
    if (jj_scan_token(DISPATCH)) return true;
    return false;
  }

 inline bool jj_3R_parameter_style_clause_5706_5_636()
 {
    if (jj_done) return true;
    if (jj_scan_token(PARAMETER)) return true;
    if (jj_scan_token(STYLE)) return true;
    if (jj_3R_parameter_style_5811_5_973()) return true;
    return false;
  }

 inline bool jj_3R_returned_result_sets_characteristic_5700_5_659()
 {
    if (jj_done) return true;
    if (jj_scan_token(DYNAMIC)) return true;
    if (jj_scan_token(RESULT)) return true;
    if (jj_scan_token(SETS)) return true;
    return false;
  }

 inline bool jj_3_1587()
 {
    if (jj_done) return true;
    if (jj_scan_token(OLD)) return true;
    if (jj_scan_token(SAVEPOINT)) return true;
    if (jj_scan_token(LEVEL)) return true;
    return false;
  }

 inline bool jj_3R_savepoint_level_indication_5693_5_660()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1586()) {
    jj_scanpos = xsp;
    if (jj_3_1587()) return true;
    }
    return false;
  }

 inline bool jj_3_1586()
 {
    if (jj_done) return true;
    if (jj_scan_token(NEW)) return true;
    if (jj_scan_token(SAVEPOINT)) return true;
    if (jj_scan_token(LEVEL)) return true;
    return false;
  }

 inline bool jj_3_1572()
 {
    if (jj_done) return true;
    if (jj_scan_token(CONSTRUCTOR)) return true;
    return false;
  }

 inline bool jj_3_1585()
 {
    if (jj_done) return true;
    if (jj_3R_savepoint_level_indication_5693_5_660()) return true;
    return false;
  }

 inline bool jj_3_1584()
 {
    if (jj_done) return true;
    if (jj_3R_returned_result_sets_characteristic_5700_5_659()) return true;
    return false;
  }

 inline bool jj_3_1583()
 {
    if (jj_done) return true;
    if (jj_3R_null_call_clause_5834_5_639()) return true;
    return false;
  }

 inline bool jj_3_1582()
 {
    if (jj_done) return true;
    if (jj_3R_SQL_data_access_indication_5825_5_638()) return true;
    return false;
  }

 inline bool jj_3_1581()
 {
    if (jj_done) return true;
    if (jj_3R_deterministic_characteristic_5818_5_637()) return true;
    return false;
  }

 inline bool jj_3_1580()
 {
    if (jj_done) return true;
    if (jj_scan_token(SPECIFIC)) return true;
    if (jj_3R_schema_qualified_name_970_5_252()) return true;
    return false;
  }

 inline bool jj_3_1579()
 {
    if (jj_done) return true;
    if (jj_3R_parameter_style_clause_5706_5_636()) return true;
    return false;
  }

 inline bool jj_3R_routine_characteristic_5680_5_658()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1578()) {
    jj_scanpos = xsp;
    if (jj_3_1579()) {
    jj_scanpos = xsp;
    if (jj_3_1580()) {
    jj_scanpos = xsp;
    if (jj_3_1581()) {
    jj_scanpos = xsp;
    if (jj_3_1582()) {
    jj_scanpos = xsp;
    if (jj_3_1583()) {
    jj_scanpos = xsp;
    if (jj_3_1584()) {
    jj_scanpos = xsp;
    if (jj_3_1585()) return true;
    }
    }
    }
    }
    }
    }
    }
    return false;
  }

 inline bool jj_3_1571()
 {
    if (jj_done) return true;
    if (jj_scan_token(STATIC)) return true;
    return false;
  }

 inline bool jj_3_1578()
 {
    if (jj_done) return true;
    if (jj_3R_language_clause_3935_5_635()) return true;
    return false;
  }

 inline bool jj_3_1577()
 {
    if (jj_done) return true;
    if (jj_3R_routine_characteristic_5680_5_658()) return true;
    return false;
  }

 inline bool jj_3_1574()
 {
    if (jj_done) return true;
    if (jj_3R_returns_clause_5718_5_656()) return true;
    return false;
  }

 inline bool jj_3_1570()
 {
    if (jj_done) return true;
    if (jj_scan_token(INSTANCE)) return true;
    return false;
  }

 inline bool jj_3_1573()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1570()) {
    jj_scanpos = xsp;
    if (jj_3_1571()) {
    jj_scanpos = xsp;
    if (jj_3_1572()) return true;
    }
    }
    return false;
  }

 inline bool jj_3_1576()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1573()) jj_scanpos = xsp;
    if (jj_scan_token(METHOD)) return true;
    if (jj_3R_identifier_928_3_141()) return true;
    if (jj_3R_SQL_parameter_declaration_list_5611_6_657()) return true;
    return false;
  }

 inline bool jj_3R_method_specification_designator_5664_5_649()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1575()) {
    jj_scanpos = xsp;
    if (jj_3_1576()) return true;
    }
    return false;
  }

 inline bool jj_3_1575()
 {
    if (jj_done) return true;
    if (jj_scan_token(SPECIFIC)) return true;
    if (jj_scan_token(METHOD)) return true;
    if (jj_3R_specific_identifier_5475_5_633()) return true;
    return false;
  }

 inline bool jj_3_1569()
 {
    if (jj_done) return true;
    if (jj_3R_dispatch_clause_5712_5_655()) return true;
    return false;
  }

 inline bool jj_3_1568()
 {
    if (jj_done) return true;
    if (jj_3R_routine_description_8009_5_654()) return true;
    return false;
  }

 inline bool jj_3_1567()
 {
    if (jj_done) return true;
    if (jj_3R_locator_indication_5648_5_653()) return true;
    return false;
  }

 inline bool jj_3R_function_specification_5654_5_648()
 {
    if (jj_done) return true;
    if (jj_scan_token(FUNCTION)) return true;
    if (jj_3R_schema_qualified_name_970_5_252()) return true;
    if (jj_3R_SQL_parameter_declaration_list_5611_6_657()) return true;
    return false;
  }

 inline bool jj_3R_locator_indication_5648_5_653()
 {
    if (jj_done) return true;
    if (jj_scan_token(AS)) return true;
    if (jj_scan_token(LOCATOR)) return true;
    return false;
  }

 inline bool jj_3R_parameter_type_5642_5_977()
 {
    if (jj_done) return true;
    if (jj_3R_data_type_1086_3_251()) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1567()) jj_scanpos = xsp;
    return false;
  }

 inline bool jj_3_1560()
 {
    if (jj_done) return true;
    if (jj_scan_token(RESULT)) return true;
    return false;
  }

 inline bool jj_3_1566()
 {
    if (jj_done) return true;
    if (jj_scan_token(INOUT)) return true;
    return false;
  }

 inline bool jj_3_1565()
 {
    if (jj_done) return true;
    if (jj_scan_token(OUT)) return true;
    return false;
  }

 inline bool jj_3R_parameter_mode_5634_5_651()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1564()) {
    jj_scanpos = xsp;
    if (jj_3_1565()) {
    jj_scanpos = xsp;
    if (jj_3_1566()) return true;
    }
    }
    return false;
  }

 inline bool jj_3_1555()
 {
    if (jj_done) return true;
    if (jj_3R_method_specification_designator_5664_5_649()) return true;
    return false;
  }

 inline bool jj_3_1564()
 {
    if (jj_done) return true;
    if (jj_scan_token(IN)) return true;
    return false;
  }

 inline bool jj_3_1563()
 {
    if (jj_done) return true;
    if (jj_3R_contextually_typed_value_specification_1475_5_194()) return true;
    return false;
  }

 inline bool jj_3R_parameter_default_5627_5_652()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1562()) {
    jj_scanpos = xsp;
    if (jj_3_1563()) return true;
    }
    return false;
  }

 inline bool jj_3_1562()
 {
    if (jj_done) return true;
    if (jj_3R_value_expression_1855_5_178()) return true;
    return false;
  }

 inline bool jj_3_1561()
 {
    if (jj_done) return true;
    if (jj_scan_token(DEFAULT_)) return true;
    if (jj_3R_parameter_default_5627_5_652()) return true;
    return false;
  }

 inline bool jj_3_1559()
 {
    if (jj_done) return true;
    if (jj_3R_identifier_928_3_141()) return true;
    return false;
  }

 inline bool jj_3_1558()
 {
    if (jj_done) return true;
    if (jj_3R_parameter_mode_5634_5_651()) return true;
    return false;
  }

 inline bool jj_3R_SQL_parameter_declaration_5618_5_650()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1558()) jj_scanpos = xsp;
    xsp = jj_scanpos;
    if (jj_3_1559()) jj_scanpos = xsp;
    if (jj_3R_parameter_type_5642_5_977()) return true;
    xsp = jj_scanpos;
    if (jj_3_1560()) jj_scanpos = xsp;
    xsp = jj_scanpos;
    if (jj_3_1561()) jj_scanpos = xsp;
    return false;
  }

 inline bool jj_3_1557()
 {
    if (jj_done) return true;
    if (jj_3R_SQL_parameter_declaration_5618_5_650()) return true;
    Token * xsp;
    while (true) {
      xsp = jj_scanpos;
      if (jj_3_1556()) { jj_scanpos = xsp; break; }
    }
    return false;
  }

 inline bool jj_3_1556()
 {
    if (jj_done) return true;
    if (jj_scan_token(570)) return true;
    if (jj_3R_SQL_parameter_declaration_5618_5_650()) return true;
    return false;
  }

 inline bool jj_3R_SQL_parameter_declaration_list_5611_6_657()
 {
    if (jj_done) return true;
    if (jj_scan_token(lparen)) return true;
    return false;
  }

 inline bool jj_3_1554()
 {
    if (jj_done) return true;
    if (jj_3R_function_specification_5654_5_648()) return true;
    return false;
  }

 inline bool jj_3R_SQL_invoked_function_5605_5_976()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1554()) {
    jj_scanpos = xsp;
    if (jj_3_1555()) return true;
    }
    return false;
  }

 inline bool jj_3R_SQL_invoked_procedure_5597_5_975()
 {
    if (jj_done) return true;
    if (jj_scan_token(PROCEDURE)) return true;
    if (jj_3R_schema_qualified_name_970_5_252()) return true;
    return false;
  }

 inline bool jj_3_1553()
 {
    if (jj_done) return true;
    if (jj_3R_or_replace_8057_5_593()) return true;
    return false;
  }

 inline bool jj_3R_schema_function_5589_5_647()
 {
    if (jj_done) return true;
    if (jj_scan_token(CREATE)) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1553()) jj_scanpos = xsp;
    if (jj_3R_SQL_invoked_function_5605_5_976()) return true;
    return false;
  }

 inline bool jj_3R_schema_procedure_5583_5_646()
 {
    if (jj_done) return true;
    if (jj_scan_token(CREATE)) return true;
    if (jj_3R_SQL_invoked_procedure_5597_5_975()) return true;
    return false;
  }

 inline bool jj_3_1549()
 {
    if (jj_done) return true;
    if (jj_scan_token(CONSTRUCTOR)) return true;
    return false;
  }

 inline bool jj_3_1552()
 {
    if (jj_done) return true;
    if (jj_3R_schema_function_5589_5_647()) return true;
    return false;
  }

 inline bool jj_3R_schema_routine_5576_5_516()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1551()) {
    jj_scanpos = xsp;
    if (jj_3_1552()) return true;
    }
    return false;
  }

 inline bool jj_3_1551()
 {
    if (jj_done) return true;
    if (jj_3R_schema_procedure_5583_5_646()) return true;
    return false;
  }

 inline bool jj_3_1548()
 {
    if (jj_done) return true;
    if (jj_scan_token(STATIC)) return true;
    return false;
  }

 inline bool jj_3R_SQL_invoked_routine_5570_5_740()
 {
    if (jj_done) return true;
    if (jj_3R_schema_routine_5576_5_516()) return true;
    return false;
  }

 inline bool jj_3R_drop_data_type_statement_5564_5_758()
 {
    if (jj_done) return true;
    if (jj_scan_token(DROP)) return true;
    if (jj_scan_token(TYPE)) return true;
    if (jj_3R_schema_resolved_user_defined_type_name_1026_5_479()) return true;
    return false;
  }

 inline bool jj_3_1547()
 {
    if (jj_done) return true;
    if (jj_scan_token(INSTANCE)) return true;
    return false;
  }

 inline bool jj_3_1550()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1547()) {
    jj_scanpos = xsp;
    if (jj_3_1548()) {
    jj_scanpos = xsp;
    if (jj_3_1549()) return true;
    }
    }
    return false;
  }

 inline bool jj_3R_specific_method_specification_designator_5557_5_974()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1550()) jj_scanpos = xsp;
    if (jj_scan_token(METHOD)) return true;
    if (jj_3R_identifier_928_3_141()) return true;
    return false;
  }

 inline bool jj_3R_drop_method_specification_5551_5_645()
 {
    if (jj_done) return true;
    if (jj_scan_token(DROP)) return true;
    if (jj_3R_specific_method_specification_designator_5557_5_974()) return true;
    return false;
  }

 inline bool jj_3R_add_overriding_method_specification_5545_5_644()
 {
    if (jj_done) return true;
    if (jj_scan_token(ADD)) return true;
    if (jj_3R_overriding_method_specification_5460_5_631()) return true;
    return false;
  }

 inline bool jj_3R_add_original_method_specification_5539_5_643()
 {
    if (jj_done) return true;
    if (jj_scan_token(ADD)) return true;
    if (jj_3R_original_method_specification_5453_5_630()) return true;
    return false;
  }

 inline bool jj_3R_drop_attribute_definition_5533_5_642()
 {
    if (jj_done) return true;
    if (jj_scan_token(DROP)) return true;
    if (jj_scan_token(ATTRIBUTE)) return true;
    if (jj_3R_identifier_928_3_141()) return true;
    return false;
  }

 inline bool jj_3R_add_attribute_definition_5527_5_641()
 {
    if (jj_done) return true;
    if (jj_scan_token(ADD)) return true;
    if (jj_scan_token(ATTRIBUTE)) return true;
    if (jj_3R_attribute_definition_5497_5_969()) return true;
    return false;
  }

 inline bool jj_3_1546()
 {
    if (jj_done) return true;
    if (jj_3R_drop_method_specification_5551_5_645()) return true;
    return false;
  }

 inline bool jj_3_1545()
 {
    if (jj_done) return true;
    if (jj_3R_add_overriding_method_specification_5545_5_644()) return true;
    return false;
  }

 inline bool jj_3_1544()
 {
    if (jj_done) return true;
    if (jj_3R_add_original_method_specification_5539_5_643()) return true;
    return false;
  }

 inline bool jj_3_1543()
 {
    if (jj_done) return true;
    if (jj_3R_drop_attribute_definition_5533_5_642()) return true;
    return false;
  }

 inline bool jj_3_1542()
 {
    if (jj_done) return true;
    if (jj_3R_add_attribute_definition_5527_5_641()) return true;
    return false;
  }

 inline bool jj_3R_alter_type_statement_5511_5_757()
 {
    if (jj_done) return true;
    if (jj_scan_token(ALTER)) return true;
    if (jj_scan_token(TYPE)) return true;
    if (jj_3R_schema_resolved_user_defined_type_name_1026_5_479()) return true;
    return false;
  }

 inline bool jj_3_1526()
 {
    if (jj_done) return true;
    if (jj_scan_token(SELF)) return true;
    if (jj_scan_token(AS)) return true;
    if (jj_scan_token(LOCATOR)) return true;
    return false;
  }

 inline bool jj_3R_attribute_default_5505_5_640()
 {
    if (jj_done) return true;
    if (jj_3R_default_clause_4682_5_540()) return true;
    return false;
  }

 inline bool jj_3_1541()
 {
    if (jj_done) return true;
    if (jj_3R_collate_clause_4076_5_153()) return true;
    return false;
  }

 inline bool jj_3_1540()
 {
    if (jj_done) return true;
    if (jj_3R_attribute_default_5505_5_640()) return true;
    return false;
  }

 inline bool jj_3R_attribute_definition_5497_5_969()
 {
    if (jj_done) return true;
    if (jj_3R_identifier_928_3_141()) return true;
    if (jj_3R_data_type_1086_3_251()) return true;
    return false;
  }

 inline bool jj_3_1530()
 {
    if (jj_done) return true;
    if (jj_scan_token(CONSTRUCTOR)) return true;
    return false;
  }

 inline bool jj_3_1539()
 {
    if (jj_done) return true;
    if (jj_3R_null_call_clause_5834_5_639()) return true;
    return false;
  }

 inline bool jj_3_1538()
 {
    if (jj_done) return true;
    if (jj_3R_SQL_data_access_indication_5825_5_638()) return true;
    return false;
  }

 inline bool jj_3_1537()
 {
    if (jj_done) return true;
    if (jj_3R_deterministic_characteristic_5818_5_637()) return true;
    return false;
  }

 inline bool jj_3_1536()
 {
    if (jj_done) return true;
    if (jj_3R_parameter_style_clause_5706_5_636()) return true;
    return false;
  }

 inline bool jj_3R_method_characteristic_5487_5_634()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1535()) {
    jj_scanpos = xsp;
    if (jj_3_1536()) {
    jj_scanpos = xsp;
    if (jj_3_1537()) {
    jj_scanpos = xsp;
    if (jj_3_1538()) {
    jj_scanpos = xsp;
    if (jj_3_1539()) return true;
    }
    }
    }
    }
    return false;
  }

 inline bool jj_3_1535()
 {
    if (jj_done) return true;
    if (jj_3R_language_clause_3935_5_635()) return true;
    return false;
  }

 inline bool jj_3_1525()
 {
    if (jj_done) return true;
    if (jj_scan_token(SELF)) return true;
    if (jj_scan_token(AS)) return true;
    if (jj_scan_token(RESULT)) return true;
    return false;
  }

 inline bool jj_3_1534()
 {
    if (jj_done) return true;
    if (jj_3R_method_characteristic_5487_5_634()) return true;
    return false;
  }

 inline bool jj_3R_method_characteristics_5481_5_632()
 {
    if (jj_done) return true;
    Token * xsp;
    if (jj_3_1534()) return true;
    while (true) {
      xsp = jj_scanpos;
      if (jj_3_1534()) { jj_scanpos = xsp; break; }
    }
    return false;
  }

 inline bool jj_3_1529()
 {
    if (jj_done) return true;
    if (jj_scan_token(STATIC)) return true;
    return false;
  }

 inline bool jj_3_1533()
 {
    if (jj_done) return true;
    if (jj_3R_schema_name_956_5_140()) return true;
    if (jj_scan_token(569)) return true;
    return false;
  }

 inline bool jj_3R_specific_identifier_5475_5_633()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1533()) jj_scanpos = xsp;
    if (jj_3R_identifier_928_3_141()) return true;
    return false;
  }

 inline bool jj_3_1532()
 {
    if (jj_done) return true;
    if (jj_scan_token(SPECIFIC)) return true;
    if (jj_3R_specific_identifier_5475_5_633()) return true;
    return false;
  }

 inline bool jj_3_1528()
 {
    if (jj_done) return true;
    if (jj_scan_token(INSTANCE)) return true;
    return false;
  }

 inline bool jj_3_1531()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1528()) {
    jj_scanpos = xsp;
    if (jj_3_1529()) {
    jj_scanpos = xsp;
    if (jj_3_1530()) return true;
    }
    }
    return false;
  }

 inline bool jj_3R_partial_method_specification_5466_5_971()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1531()) jj_scanpos = xsp;
    if (jj_scan_token(METHOD)) return true;
    if (jj_3R_identifier_928_3_141()) return true;
    if (jj_3R_SQL_parameter_declaration_list_5611_6_657()) return true;
    return false;
  }

 inline bool jj_3_1522()
 {
    if (jj_done) return true;
    if (jj_scan_token(570)) return true;
    if (jj_3R_method_specification_5446_5_629()) return true;
    return false;
  }

 inline bool jj_3R_overriding_method_specification_5460_5_631()
 {
    if (jj_done) return true;
    if (jj_scan_token(OVERRIDING)) return true;
    if (jj_3R_partial_method_specification_5466_5_971()) return true;
    return false;
  }

 inline bool jj_3_1527()
 {
    if (jj_done) return true;
    if (jj_3R_method_characteristics_5481_5_632()) return true;
    return false;
  }

 inline bool jj_3R_original_method_specification_5453_5_630()
 {
    if (jj_done) return true;
    if (jj_3R_partial_method_specification_5466_5_971()) return true;
    return false;
  }

 inline bool jj_3_1524()
 {
    if (jj_done) return true;
    if (jj_3R_overriding_method_specification_5460_5_631()) return true;
    return false;
  }

 inline bool jj_3R_method_specification_5446_5_629()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1523()) {
    jj_scanpos = xsp;
    if (jj_3_1524()) return true;
    }
    return false;
  }

 inline bool jj_3_1523()
 {
    if (jj_done) return true;
    if (jj_3R_original_method_specification_5453_5_630()) return true;
    return false;
  }

 inline bool jj_3R_method_specification_list_5440_5_615()
 {
    if (jj_done) return true;
    if (jj_3R_method_specification_5446_5_629()) return true;
    return false;
  }

 inline bool jj_3_1521()
 {
    if (jj_done) return true;
    if (jj_scan_token(570)) return true;
    if (jj_3R_identifier_928_3_141()) return true;
    return false;
  }

 inline bool jj_3R_cast_to_source_5433_5_623()
 {
    if (jj_done) return true;
    if (jj_scan_token(CAST)) return true;
    if (jj_scan_token(lparen)) return true;
    if (jj_scan_token(DISTINCT)) return true;
    return false;
  }

 inline bool jj_3R_cast_to_distinct_5426_5_622()
 {
    if (jj_done) return true;
    if (jj_scan_token(CAST)) return true;
    if (jj_scan_token(lparen)) return true;
    if (jj_scan_token(SOURCE)) return true;
    return false;
  }

 inline bool jj_3R_list_of_attributes_5420_6_970()
 {
    if (jj_done) return true;
    if (jj_scan_token(lparen)) return true;
    return false;
  }

 inline bool jj_3R_cast_to_type_5414_5_621()
 {
    if (jj_done) return true;
    if (jj_scan_token(CAST)) return true;
    if (jj_scan_token(lparen)) return true;
    if (jj_scan_token(REF)) return true;
    return false;
  }

 inline bool jj_3R_cast_to_ref_5408_5_620()
 {
    if (jj_done) return true;
    if (jj_scan_token(CAST)) return true;
    if (jj_scan_token(lparen)) return true;
    if (jj_scan_token(SOURCE)) return true;
    return false;
  }

 inline bool jj_3R_system_generated_representation_5402_5_628()
 {
    if (jj_done) return true;
    if (jj_scan_token(REF)) return true;
    if (jj_scan_token(IS)) return true;
    if (jj_scan_token(SYSTEM)) return true;
    return false;
  }

 inline bool jj_3R_derived_representation_5396_5_627()
 {
    if (jj_done) return true;
    if (jj_scan_token(REF)) return true;
    if (jj_scan_token(FROM)) return true;
    if (jj_3R_list_of_attributes_5420_6_970()) return true;
    return false;
  }

 inline bool jj_3R_user_defined_representation_5390_5_626()
 {
    if (jj_done) return true;
    if (jj_scan_token(REF)) return true;
    if (jj_scan_token(USING)) return true;
    if (jj_3R_predefined_type_1100_5_147()) return true;
    return false;
  }

 inline bool jj_3_1520()
 {
    if (jj_done) return true;
    if (jj_3R_system_generated_representation_5402_5_628()) return true;
    return false;
  }

 inline bool jj_3_1519()
 {
    if (jj_done) return true;
    if (jj_3R_derived_representation_5396_5_627()) return true;
    return false;
  }

 inline bool jj_3R_reference_type_specification_5382_5_619()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1518()) {
    jj_scanpos = xsp;
    if (jj_3_1519()) {
    jj_scanpos = xsp;
    if (jj_3_1520()) return true;
    }
    }
    return false;
  }

 inline bool jj_3_1518()
 {
    if (jj_done) return true;
    if (jj_3R_user_defined_representation_5390_5_626()) return true;
    return false;
  }

 inline bool jj_3_1517()
 {
    if (jj_done) return true;
    if (jj_scan_token(NOT)) return true;
    if (jj_scan_token(FINAL)) return true;
    return false;
  }

 inline bool jj_3R_finality_5375_5_618()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1516()) {
    jj_scanpos = xsp;
    if (jj_3_1517()) return true;
    }
    return false;
  }

 inline bool jj_3_1516()
 {
    if (jj_done) return true;
    if (jj_scan_token(FINAL)) return true;
    return false;
  }

 inline bool jj_3_1513()
 {
    if (jj_done) return true;
    if (jj_scan_token(570)) return true;
    if (jj_3R_member_5362_5_625()) return true;
    return false;
  }

 inline bool jj_3_1515()
 {
    if (jj_done) return true;
    if (jj_scan_token(NOT)) return true;
    if (jj_scan_token(INSTANTIABLE)) return true;
    return false;
  }

 inline bool jj_3R_instantiable_clause_5368_5_617()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1514()) {
    jj_scanpos = xsp;
    if (jj_3_1515()) return true;
    }
    return false;
  }

 inline bool jj_3_1514()
 {
    if (jj_done) return true;
    if (jj_scan_token(INSTANTIABLE)) return true;
    return false;
  }

 inline bool jj_3R_member_5362_5_625()
 {
    if (jj_done) return true;
    if (jj_3R_attribute_definition_5497_5_969()) return true;
    return false;
  }

 inline bool jj_3R_member_list_5356_6_624()
 {
    if (jj_done) return true;
    if (jj_scan_token(lparen)) return true;
    if (jj_3R_member_5362_5_625()) return true;
    return false;
  }

 inline bool jj_3_1512()
 {
    if (jj_done) return true;
    if (jj_3R_member_list_5356_6_624()) return true;
    return false;
  }

 inline bool jj_3_1511()
 {
    if (jj_done) return true;
    if (jj_3R_data_type_1086_3_251()) return true;
    return false;
  }

 inline bool jj_3R_representation_5348_5_613()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1510()) {
    jj_scanpos = xsp;
    if (jj_3_1511()) {
    jj_scanpos = xsp;
    if (jj_3_1512()) return true;
    }
    }
    return false;
  }

 inline bool jj_3_1510()
 {
    if (jj_done) return true;
    if (jj_3R_predefined_type_1100_5_147()) return true;
    return false;
  }

 inline bool jj_3_1502()
 {
    if (jj_done) return true;
    if (jj_3R_user_defined_type_option_5324_5_616()) return true;
    return false;
  }

 inline bool jj_3R_supertype_name_5342_5_968()
 {
    if (jj_done) return true;
    if (jj_3R_path_resolved_user_defined_type_name_1281_5_151()) return true;
    return false;
  }

 inline bool jj_3R_subtype_clause_5336_5_612()
 {
    if (jj_done) return true;
    if (jj_scan_token(UNDER)) return true;
    if (jj_3R_supertype_name_5342_5_968()) return true;
    return false;
  }

 inline bool jj_3_1509()
 {
    if (jj_done) return true;
    if (jj_3R_cast_to_source_5433_5_623()) return true;
    return false;
  }

 inline bool jj_3_1508()
 {
    if (jj_done) return true;
    if (jj_3R_cast_to_distinct_5426_5_622()) return true;
    return false;
  }

 inline bool jj_3_1507()
 {
    if (jj_done) return true;
    if (jj_3R_cast_to_type_5414_5_621()) return true;
    return false;
  }

 inline bool jj_3_1506()
 {
    if (jj_done) return true;
    if (jj_3R_cast_to_ref_5408_5_620()) return true;
    return false;
  }

 inline bool jj_3_1505()
 {
    if (jj_done) return true;
    if (jj_3R_reference_type_specification_5382_5_619()) return true;
    return false;
  }

 inline bool jj_3_1504()
 {
    if (jj_done) return true;
    if (jj_3R_finality_5375_5_618()) return true;
    return false;
  }

 inline bool jj_3R_user_defined_type_option_5324_5_616()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1503()) {
    jj_scanpos = xsp;
    if (jj_3_1504()) {
    jj_scanpos = xsp;
    if (jj_3_1505()) {
    jj_scanpos = xsp;
    if (jj_3_1506()) {
    jj_scanpos = xsp;
    if (jj_3_1507()) {
    jj_scanpos = xsp;
    if (jj_3_1508()) {
    jj_scanpos = xsp;
    if (jj_3_1509()) return true;
    }
    }
    }
    }
    }
    }
    return false;
  }

 inline bool jj_3_1503()
 {
    if (jj_done) return true;
    if (jj_3R_instantiable_clause_5368_5_617()) return true;
    return false;
  }

 inline bool jj_3R_user_defined_type_option_list_5318_5_614()
 {
    if (jj_done) return true;
    if (jj_3R_user_defined_type_option_5324_5_616()) return true;
    Token * xsp;
    while (true) {
      xsp = jj_scanpos;
      if (jj_3_1502()) { jj_scanpos = xsp; break; }
    }
    return false;
  }

 inline bool jj_3_1501()
 {
    if (jj_done) return true;
    if (jj_3R_method_specification_list_5440_5_615()) return true;
    return false;
  }

 inline bool jj_3_1500()
 {
    if (jj_done) return true;
    if (jj_3R_user_defined_type_option_list_5318_5_614()) return true;
    return false;
  }

 inline bool jj_3_1499()
 {
    if (jj_done) return true;
    if (jj_scan_token(AS)) return true;
    if (jj_3R_representation_5348_5_613()) return true;
    return false;
  }

 inline bool jj_3_1498()
 {
    if (jj_done) return true;
    if (jj_3R_subtype_clause_5336_5_612()) return true;
    return false;
  }

 inline bool jj_3R_user_defined_type_body_5308_5_956()
 {
    if (jj_done) return true;
    if (jj_3R_schema_resolved_user_defined_type_name_1026_5_479()) return true;
    return false;
  }

 inline bool jj_3_1491()
 {
    if (jj_done) return true;
    if (jj_scan_token(AS)) return true;
    return false;
  }

 inline bool jj_3_1493()
 {
    if (jj_done) return true;
    if (jj_scan_token(AS)) return true;
    return false;
  }

 inline bool jj_3_1489()
 {
    if (jj_done) return true;
    if (jj_scan_token(AS)) return true;
    return false;
  }

 inline bool jj_3_1492()
 {
    if (jj_done) return true;
    if (jj_scan_token(AS)) return true;
    return false;
  }

 inline bool jj_3R_user_defined_type_definition_5302_5_512()
 {
    if (jj_done) return true;
    if (jj_scan_token(CREATE)) return true;
    if (jj_scan_token(TYPE)) return true;
    if (jj_3R_user_defined_type_body_5308_5_956()) return true;
    return false;
  }

 inline bool jj_3R_drop_trigger_statement_5296_5_756()
 {
    if (jj_done) return true;
    if (jj_scan_token(DROP)) return true;
    if (jj_scan_token(TRIGGER)) return true;
    if (jj_3R_schema_qualified_name_970_5_252()) return true;
    return false;
  }

 inline bool jj_3_1490()
 {
    if (jj_done) return true;
    if (jj_scan_token(ROW)) return true;
    return false;
  }

 inline bool jj_3_1488()
 {
    if (jj_done) return true;
    if (jj_scan_token(ROW)) return true;
    return false;
  }

 inline bool jj_3_1484()
 {
    if (jj_done) return true;
    if (jj_3R_SQL_procedure_statement_6507_5_610()) return true;
    if (jj_scan_token(semicolon)) return true;
    return false;
  }

 inline bool jj_3_1497()
 {
    if (jj_done) return true;
    if (jj_scan_token(NEW)) return true;
    if (jj_scan_token(TABLE)) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1493()) jj_scanpos = xsp;
    if (jj_3R_identifier_928_3_141()) return true;
    return false;
  }

 inline bool jj_3_1496()
 {
    if (jj_done) return true;
    if (jj_scan_token(OLD)) return true;
    if (jj_scan_token(TABLE)) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1492()) jj_scanpos = xsp;
    if (jj_3R_identifier_928_3_141()) return true;
    return false;
  }

 inline bool jj_3_1495()
 {
    if (jj_done) return true;
    if (jj_scan_token(NEW)) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1490()) jj_scanpos = xsp;
    xsp = jj_scanpos;
    if (jj_3_1491()) jj_scanpos = xsp;
    if (jj_3R_identifier_928_3_141()) return true;
    return false;
  }

 inline bool jj_3R_transition_table_or_variable_5287_5_611()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1494()) {
    jj_scanpos = xsp;
    if (jj_3_1495()) {
    jj_scanpos = xsp;
    if (jj_3_1496()) {
    jj_scanpos = xsp;
    if (jj_3_1497()) return true;
    }
    }
    }
    return false;
  }

 inline bool jj_3_1494()
 {
    if (jj_done) return true;
    if (jj_scan_token(OLD)) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1488()) jj_scanpos = xsp;
    xsp = jj_scanpos;
    if (jj_3_1489()) jj_scanpos = xsp;
    if (jj_3R_identifier_928_3_141()) return true;
    return false;
  }

 inline bool jj_3_1481()
 {
    if (jj_done) return true;
    if (jj_scan_token(STATEMENT)) return true;
    return false;
  }

 inline bool jj_3_1487()
 {
    if (jj_done) return true;
    if (jj_3R_transition_table_or_variable_5287_5_611()) return true;
    return false;
  }

 inline bool jj_3R_transition_table_or_variable_list_5281_5_607()
 {
    if (jj_done) return true;
    Token * xsp;
    if (jj_3_1487()) return true;
    while (true) {
      xsp = jj_scanpos;
      if (jj_3_1487()) { jj_scanpos = xsp; break; }
    }
    return false;
  }

 inline bool jj_3_1480()
 {
    if (jj_done) return true;
    if (jj_scan_token(ROW)) return true;
    return false;
  }

 inline bool jj_3_1486()
 {
    if (jj_done) return true;
    if (jj_scan_token(BEGIN)) return true;
    if (jj_scan_token(ATOMIC)) return true;
    Token * xsp;
    if (jj_3_1484()) return true;
    while (true) {
      xsp = jj_scanpos;
      if (jj_3_1484()) { jj_scanpos = xsp; break; }
    }
    return false;
  }

 inline bool jj_3_1485()
 {
    if (jj_done) return true;
    if (jj_3R_SQL_procedure_statement_6507_5_610()) return true;
    return false;
  }

 inline bool jj_3_1471()
 {
    if (jj_done) return true;
    if (jj_3R_drop_behavior_4366_5_592()) return true;
    return false;
  }

 inline bool jj_3R_triggered_when_clause_5268_5_609()
 {
    if (jj_done) return true;
    if (jj_scan_token(WHEN)) return true;
    if (jj_scan_token(lparen)) return true;
    if (jj_3R_search_condition_3868_5_818()) return true;
    return false;
  }

 inline bool jj_3_1483()
 {
    if (jj_done) return true;
    if (jj_3R_triggered_when_clause_5268_5_609()) return true;
    return false;
  }

 inline bool jj_3_1482()
 {
    if (jj_done) return true;
    if (jj_scan_token(FOR)) return true;
    if (jj_scan_token(EACH)) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1480()) {
    jj_scanpos = xsp;
    if (jj_3_1481()) return true;
    }
    return false;
  }

 inline bool jj_3_1476()
 {
    if (jj_done) return true;
    if (jj_scan_token(OF)) return true;
    if (jj_3R_trigger_column_list_5254_5_608()) return true;
    return false;
  }

 inline bool jj_3R_trigger_column_list_5254_5_608()
 {
    if (jj_done) return true;
    if (jj_3R_column_name_list_2942_5_191()) return true;
    return false;
  }

 inline bool jj_3_1472()
 {
    if (jj_done) return true;
    if (jj_scan_token(REFERENCING)) return true;
    if (jj_3R_transition_table_or_variable_list_5281_5_607()) return true;
    return false;
  }

 inline bool jj_3_1479()
 {
    if (jj_done) return true;
    if (jj_scan_token(UPDATE)) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1476()) jj_scanpos = xsp;
    return false;
  }

 inline bool jj_3_1478()
 {
    if (jj_done) return true;
    if (jj_scan_token(DELETE)) return true;
    return false;
  }

 inline bool jj_3_1477()
 {
    if (jj_done) return true;
    if (jj_scan_token(INSERT)) return true;
    return false;
  }

 inline bool jj_3_1475()
 {
    if (jj_done) return true;
    if (jj_scan_token(INSTEAD)) return true;
    if (jj_scan_token(OF)) return true;
    return false;
  }

 inline bool jj_3_1474()
 {
    if (jj_done) return true;
    if (jj_scan_token(AFTER)) return true;
    return false;
  }

 inline bool jj_3_1473()
 {
    if (jj_done) return true;
    if (jj_scan_token(BEFORE)) return true;
    return false;
  }

 inline bool jj_3R_trigger_definition_5230_5_511()
 {
    if (jj_done) return true;
    if (jj_scan_token(CREATE)) return true;
    if (jj_scan_token(TRIGGER)) return true;
    if (jj_3R_schema_qualified_name_970_5_252()) return true;
    return false;
  }

 inline bool jj_3R_drop_assertion_statement_5224_5_755()
 {
    if (jj_done) return true;
    if (jj_scan_token(DROP)) return true;
    if (jj_scan_token(ASSERTION)) return true;
    if (jj_3R_schema_qualified_name_970_5_252()) return true;
    return false;
  }

 inline bool jj_3_1470()
 {
    if (jj_done) return true;
    if (jj_3R_constraint_characteristics_4088_5_556()) return true;
    return false;
  }

 inline bool jj_3R_assertion_definition_5216_5_510()
 {
    if (jj_done) return true;
    if (jj_scan_token(CREATE)) return true;
    if (jj_scan_token(ASSERTION)) return true;
    if (jj_3R_schema_qualified_name_970_5_252()) return true;
    return false;
  }

 inline bool jj_3R_drop_transliteration_statement_5210_5_754()
 {
    if (jj_done) return true;
    if (jj_scan_token(DROP)) return true;
    if (jj_scan_token(TRANSLATION)) return true;
    if (jj_3R_schema_qualified_name_970_5_252()) return true;
    return false;
  }

 inline bool jj_3R_transliteration_routine_5204_5_606()
 {
    if (jj_done) return true;
    if (jj_3R_specific_routine_designator_4041_5_708()) return true;
    return false;
  }

 inline bool jj_3_1469()
 {
    if (jj_done) return true;
    if (jj_3R_transliteration_routine_5204_5_606()) return true;
    return false;
  }

 inline bool jj_3_1468()
 {
    if (jj_done) return true;
    if (jj_3R_schema_qualified_name_970_5_252()) return true;
    return false;
  }

 inline bool jj_3_1465()
 {
    if (jj_done) return true;
    if (jj_3R_pad_characteristic_5164_5_605()) return true;
    return false;
  }

 inline bool jj_3_1463()
 {
    if (jj_done) return true;
    if (jj_scan_token(AS)) return true;
    return false;
  }

 inline bool jj_3R_transliteration_definition_5177_5_509()
 {
    if (jj_done) return true;
    if (jj_scan_token(CREATE)) return true;
    if (jj_scan_token(TRANSLATION)) return true;
    if (jj_3R_schema_qualified_name_970_5_252()) return true;
    return false;
  }

 inline bool jj_3R_drop_collation_statement_5171_5_753()
 {
    if (jj_done) return true;
    if (jj_scan_token(DROP)) return true;
    if (jj_scan_token(COLLATION)) return true;
    if (jj_3R_schema_qualified_name_970_5_252()) return true;
    return false;
  }

 inline bool jj_3_1467()
 {
    if (jj_done) return true;
    if (jj_scan_token(PAD)) return true;
    if (jj_scan_token(SPACE)) return true;
    return false;
  }

 inline bool jj_3R_pad_characteristic_5164_5_605()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1466()) {
    jj_scanpos = xsp;
    if (jj_3_1467()) return true;
    }
    return false;
  }

 inline bool jj_3_1464()
 {
    if (jj_done) return true;
    if (jj_3R_collate_clause_4076_5_153()) return true;
    return false;
  }

 inline bool jj_3_1466()
 {
    if (jj_done) return true;
    if (jj_scan_token(NO)) return true;
    if (jj_scan_token(PAD)) return true;
    return false;
  }

 inline bool jj_3R_collation_definition_5157_5_508()
 {
    if (jj_done) return true;
    if (jj_scan_token(CREATE)) return true;
    if (jj_scan_token(COLLATION)) return true;
    if (jj_3R_schema_qualified_name_970_5_252()) return true;
    return false;
  }

 inline bool jj_3R_drop_character_set_statement_5151_5_752()
 {
    if (jj_done) return true;
    if (jj_scan_token(DROP)) return true;
    if (jj_scan_token(CHARACTER)) return true;
    if (jj_scan_token(SET)) return true;
    return false;
  }

 inline bool jj_3R_character_set_definition_5138_5_507()
 {
    if (jj_done) return true;
    if (jj_scan_token(CREATE)) return true;
    if (jj_scan_token(CHARACTER)) return true;
    if (jj_scan_token(SET)) return true;
    return false;
  }

 inline bool jj_3R_drop_domain_statement_5132_5_751()
 {
    if (jj_done) return true;
    if (jj_scan_token(DROP)) return true;
    if (jj_scan_token(DOMAIN)) return true;
    if (jj_3R_schema_qualified_name_970_5_252()) return true;
    return false;
  }

 inline bool jj_3R_drop_domain_constraint_definition_5126_5_604()
 {
    if (jj_done) return true;
    if (jj_scan_token(DROP)) return true;
    if (jj_scan_token(CONSTRAINT)) return true;
    if (jj_3R_schema_qualified_name_970_5_252()) return true;
    return false;
  }

 inline bool jj_3_1453()
 {
    if (jj_done) return true;
    if (jj_scan_token(AS)) return true;
    return false;
  }

 inline bool jj_3R_add_domain_constraint_definition_5120_5_603()
 {
    if (jj_done) return true;
    if (jj_scan_token(ADD)) return true;
    if (jj_3R_domain_constraint_5086_5_600()) return true;
    return false;
  }

 inline bool jj_3R_drop_domain_default_clause_5114_5_602()
 {
    if (jj_done) return true;
    if (jj_scan_token(DROP)) return true;
    if (jj_scan_token(DEFAULT_)) return true;
    return false;
  }

 inline bool jj_3R_set_domain_default_clause_5108_5_601()
 {
    if (jj_done) return true;
    if (jj_scan_token(SET)) return true;
    if (jj_3R_default_clause_4682_5_540()) return true;
    return false;
  }

 inline bool jj_3_1462()
 {
    if (jj_done) return true;
    if (jj_3R_drop_domain_constraint_definition_5126_5_604()) return true;
    return false;
  }

 inline bool jj_3_1461()
 {
    if (jj_done) return true;
    if (jj_3R_add_domain_constraint_definition_5120_5_603()) return true;
    return false;
  }

 inline bool jj_3_1460()
 {
    if (jj_done) return true;
    if (jj_3R_drop_domain_default_clause_5114_5_602()) return true;
    return false;
  }

 inline bool jj_3_1459()
 {
    if (jj_done) return true;
    if (jj_3R_set_domain_default_clause_5108_5_601()) return true;
    return false;
  }

 inline bool jj_3R_alter_domain_statement_5093_5_750()
 {
    if (jj_done) return true;
    if (jj_scan_token(ALTER)) return true;
    if (jj_scan_token(DOMAIN)) return true;
    if (jj_3R_schema_qualified_name_970_5_252()) return true;
    return false;
  }

 inline bool jj_3_1457()
 {
    if (jj_done) return true;
    if (jj_3R_constraint_name_definition_4082_5_555()) return true;
    return false;
  }

 inline bool jj_3_1458()
 {
    if (jj_done) return true;
    if (jj_3R_constraint_characteristics_4088_5_556()) return true;
    return false;
  }

 inline bool jj_3R_domain_constraint_5086_5_600()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1457()) jj_scanpos = xsp;
    if (jj_3R_check_constraint_definition_4808_5_559()) return true;
    return false;
  }

 inline bool jj_3_1455()
 {
    if (jj_done) return true;
    if (jj_3R_domain_constraint_5086_5_600()) return true;
    return false;
  }

 inline bool jj_3_1456()
 {
    if (jj_done) return true;
    if (jj_3R_collate_clause_4076_5_153()) return true;
    return false;
  }

 inline bool jj_3_1454()
 {
    if (jj_done) return true;
    if (jj_3R_default_clause_4682_5_540()) return true;
    return false;
  }

 inline bool jj_3R_domain_definition_5077_5_506()
 {
    if (jj_done) return true;
    if (jj_scan_token(CREATE)) return true;
    if (jj_scan_token(DOMAIN)) return true;
    if (jj_3R_schema_qualified_name_970_5_252()) return true;
    return false;
  }

 inline bool jj_3_1446()
 {
    if (jj_done) return true;
    if (jj_3R_subview_clause_5033_5_596()) return true;
    return false;
  }

 inline bool jj_3R_drop_view_statement_5071_5_744()
 {
    if (jj_done) return true;
    if (jj_scan_token(DROP)) return true;
    if (jj_scan_token(VIEW)) return true;
    if (jj_3R_table_name_948_5_382()) return true;
    return false;
  }

 inline bool jj_3R_view_column_list_5065_5_595()
 {
    if (jj_done) return true;
    if (jj_3R_column_name_list_2942_5_191()) return true;
    return false;
  }

 inline bool jj_3_1448()
 {
    if (jj_done) return true;
    if (jj_scan_token(570)) return true;
    if (jj_3R_view_element_5045_5_598()) return true;
    return false;
  }

 inline bool jj_3_1452()
 {
    if (jj_done) return true;
    if (jj_scan_token(LOCAL)) return true;
    return false;
  }

 inline bool jj_3R_levels_clause_5058_5_594()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1451()) {
    jj_scanpos = xsp;
    if (jj_3_1452()) return true;
    }
    return false;
  }

 inline bool jj_3_1451()
 {
    if (jj_done) return true;
    if (jj_scan_token(CASCADED)) return true;
    return false;
  }

 inline bool jj_3R_view_column_option_5052_5_599()
 {
    if (jj_done) return true;
    if (jj_3R_identifier_928_3_141()) return true;
    if (jj_scan_token(WITH)) return true;
    if (jj_scan_token(OPTIONS)) return true;
    return false;
  }

 inline bool jj_3_1450()
 {
    if (jj_done) return true;
    if (jj_3R_view_column_option_5052_5_599()) return true;
    return false;
  }

 inline bool jj_3R_view_element_5045_5_598()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1449()) {
    jj_scanpos = xsp;
    if (jj_3_1450()) return true;
    }
    return false;
  }

 inline bool jj_3_1449()
 {
    if (jj_done) return true;
    if (jj_3R_self_referencing_column_specification_4493_5_538()) return true;
    return false;
  }

 inline bool jj_3_1443()
 {
    if (jj_done) return true;
    if (jj_3R_levels_clause_5058_5_594()) return true;
    return false;
  }

 inline bool jj_3R_view_element_list_5039_6_597()
 {
    if (jj_done) return true;
    if (jj_scan_token(lparen)) return true;
    if (jj_3R_view_element_5045_5_598()) return true;
    return false;
  }

 inline bool jj_3R_subview_clause_5033_5_596()
 {
    if (jj_done) return true;
    if (jj_scan_token(UNDER)) return true;
    if (jj_3R_table_name_948_5_382()) return true;
    return false;
  }

 inline bool jj_3_1444()
 {
    if (jj_done) return true;
    if (jj_scan_token(WITH)) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1443()) jj_scanpos = xsp;
    if (jj_scan_token(CHECK)) return true;
    if (jj_scan_token(OPTION)) return true;
    return false;
  }

 inline bool jj_3_1447()
 {
    if (jj_done) return true;
    if (jj_3R_view_element_list_5039_6_597()) return true;
    return false;
  }

 inline bool jj_3R_referenceable_view_specification_5026_5_1039()
 {
    if (jj_done) return true;
    if (jj_scan_token(OF)) return true;
    return false;
  }

 inline bool jj_3_1445()
 {
    if (jj_done) return true;
    if (jj_scan_token(lparen)) return true;
    if (jj_3R_view_column_list_5065_5_595()) return true;
    if (jj_scan_token(rparen)) return true;
    return false;
  }

 inline bool jj_3R_regular_view_specification_5020_5_1038()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1445()) jj_scanpos = xsp;
    return false;
  }

 inline bool jj_3R_view_specification_5014_5_1031()
 {
    if (jj_done) return true;
    if (jj_3R_referenceable_view_specification_5026_5_1039()) return true;
    return false;
  }

 inline bool jj_3_1438()
 {
    if (jj_done) return true;
    if (jj_scan_token(COLUMN)) return true;
    return false;
  }

 inline bool jj_3R_view_specification_5013_5_1030()
 {
    if (jj_done) return true;
    if (jj_3R_regular_view_specification_5020_5_1038()) return true;
    return false;
  }

 inline bool jj_3R_view_specification_5013_5_955()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3R_view_specification_5013_5_1030()) {
    jj_scanpos = xsp;
    if (jj_3R_view_specification_5014_5_1031()) return true;
    }
    return false;
  }

 inline bool jj_3_1442()
 {
    if (jj_done) return true;
    if (jj_scan_token(RECURSIVE)) return true;
    return false;
  }

 inline bool jj_3_1441()
 {
    if (jj_done) return true;
    if (jj_3R_or_replace_8057_5_593()) return true;
    return false;
  }

 inline bool jj_3R_view_definition_5004_5_505()
 {
    if (jj_done) return true;
    if (jj_scan_token(CREATE)) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1441()) jj_scanpos = xsp;
    xsp = jj_scanpos;
    if (jj_3_1442()) jj_scanpos = xsp;
    if (jj_scan_token(VIEW)) return true;
    if (jj_3R_table_name_948_5_382()) return true;
    if (jj_3R_view_specification_5013_5_955()) return true;
    if (jj_scan_token(AS)) return true;
    return false;
  }

 inline bool jj_3_1440()
 {
    if (jj_done) return true;
    if (jj_3R_drop_behavior_4366_5_592()) return true;
    return false;
  }

 inline bool jj_3_1439()
 {
    if (jj_done) return true;
    if (jj_scan_token(IF)) return true;
    if (jj_scan_token(EXISTS)) return true;
    return false;
  }

 inline bool jj_3R_drop_table_statement_4995_5_743()
 {
    if (jj_done) return true;
    if (jj_scan_token(DROP)) return true;
    if (jj_scan_token(TABLE)) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1439()) jj_scanpos = xsp;
    if (jj_3R_table_name_948_5_382()) return true;
    return false;
  }

 inline bool jj_3R_drop_system_versioning_clause_4989_5_577()
 {
    if (jj_done) return true;
    if (jj_scan_token(DROP)) return true;
    if (jj_scan_token(SYSTEM)) return true;
    if (jj_scan_token(VERSIONING)) return true;
    return false;
  }

 inline bool jj_3R_alter_system_versioning_clause_4983_5_576()
 {
    if (jj_done) return true;
    if (jj_scan_token(ALTER)) return true;
    if (jj_scan_token(SYSTEM)) return true;
    if (jj_scan_token(VERSIONING)) return true;
    return false;
  }

 inline bool jj_3_1437()
 {
    if (jj_done) return true;
    if (jj_scan_token(COLUMN)) return true;
    return false;
  }

 inline bool jj_3R_add_system_versioning_clause_4959_5_575()
 {
    if (jj_done) return true;
    if (jj_scan_token(ADD)) return true;
    if (jj_3R_system_versioning_clause_4425_5_522()) return true;
    return false;
  }

 inline bool jj_3R_drop_table_constraint_definition_4953_5_574()
 {
    if (jj_done) return true;
    if (jj_scan_token(DROP)) return true;
    if (jj_scan_token(CONSTRAINT)) return true;
    if (jj_3R_schema_qualified_name_970_5_252()) return true;
    return false;
  }

 inline bool jj_3R_alter_table_constraint_definition_4947_5_573()
 {
    if (jj_done) return true;
    if (jj_scan_token(ALTER)) return true;
    if (jj_scan_token(CONSTRAINT)) return true;
    if (jj_3R_schema_qualified_name_970_5_252()) return true;
    return false;
  }

 inline bool jj_3_1428()
 {
    if (jj_done) return true;
    if (jj_3R_alter_identity_column_option_4916_5_588()) return true;
    return false;
  }

 inline bool jj_3_1436()
 {
    if (jj_done) return true;
    if (jj_scan_token(COLUMN)) return true;
    return false;
  }

 inline bool jj_3R_add_table_constraint_definition_4941_5_572()
 {
    if (jj_done) return true;
    if (jj_scan_token(ADD)) return true;
    if (jj_3R_table_constraint_definition_4704_5_532()) return true;
    return false;
  }

 inline bool jj_3_1433()
 {
    if (jj_done) return true;
    if (jj_scan_token(BY)) return true;
    if (jj_scan_token(DEFAULT_)) return true;
    return false;
  }

 inline bool jj_3R_drop_column_definition_4935_5_571()
 {
    if (jj_done) return true;
    if (jj_scan_token(DROP)) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1436()) jj_scanpos = xsp;
    if (jj_3R_identifier_928_3_141()) return true;
    if (jj_3R_drop_behavior_4366_5_592()) return true;
    return false;
  }

 inline bool jj_3_1432()
 {
    if (jj_done) return true;
    if (jj_scan_token(ALWAYS)) return true;
    return false;
  }

 inline bool jj_3R_drop_column_generation_expression_clause_4929_5_587()
 {
    if (jj_done) return true;
    if (jj_scan_token(DROP)) return true;
    if (jj_scan_token(EXPRESSION)) return true;
    return false;
  }

 inline bool jj_3R_drop_identity_property_clause_4923_5_586()
 {
    if (jj_done) return true;
    if (jj_scan_token(DROP)) return true;
    if (jj_scan_token(IDENTITY)) return true;
    return false;
  }

 inline bool jj_3_1435()
 {
    if (jj_done) return true;
    if (jj_scan_token(SET)) return true;
    if (jj_3R_basic_sequence_generator_option_6157_5_591()) return true;
    return false;
  }

 inline bool jj_3R_alter_identity_column_option_4916_5_588()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1434()) {
    jj_scanpos = xsp;
    if (jj_3_1435()) return true;
    }
    return false;
  }

 inline bool jj_3_1434()
 {
    if (jj_done) return true;
    if (jj_3R_alter_sequence_generator_restart_option_6248_5_590()) return true;
    return false;
  }

 inline bool jj_3R_set_identity_column_generation_clause_4910_5_589()
 {
    if (jj_done) return true;
    if (jj_scan_token(SET)) return true;
    if (jj_scan_token(GENERATED)) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1432()) {
    jj_scanpos = xsp;
    if (jj_3_1433()) return true;
    }
    return false;
  }

 inline bool jj_3_1429()
 {
    if (jj_done) return true;
    if (jj_3R_alter_identity_column_option_4916_5_588()) return true;
    return false;
  }

 inline bool jj_3_1431()
 {
    if (jj_done) return true;
    Token * xsp;
    if (jj_3_1429()) return true;
    while (true) {
      xsp = jj_scanpos;
      if (jj_3_1429()) { jj_scanpos = xsp; break; }
    }
    return false;
  }

 inline bool jj_3R_alter_identity_column_specification_4903_5_585()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1430()) {
    jj_scanpos = xsp;
    if (jj_3_1431()) return true;
    }
    return false;
  }

 inline bool jj_3_1430()
 {
    if (jj_done) return true;
    if (jj_3R_set_identity_column_generation_clause_4910_5_589()) return true;
    return false;
  }

 inline bool jj_3R_alter_column_data_type_clause_4897_5_584()
 {
    if (jj_done) return true;
    if (jj_scan_token(SET)) return true;
    if (jj_scan_token(DATA)) return true;
    if (jj_scan_token(TYPE)) return true;
    return false;
  }

 inline bool jj_3R_drop_column_scope_clause_4891_5_583()
 {
    if (jj_done) return true;
    if (jj_scan_token(DROP)) return true;
    if (jj_scan_token(SCOPE)) return true;
    if (jj_3R_drop_behavior_4366_5_592()) return true;
    return false;
  }

 inline bool jj_3R_add_column_scope_clause_4885_5_582()
 {
    if (jj_done) return true;
    if (jj_scan_token(ADD)) return true;
    if (jj_3R_scope_clause_1269_5_171()) return true;
    return false;
  }

 inline bool jj_3R_drop_column_not_null_clause_4879_5_581()
 {
    if (jj_done) return true;
    if (jj_scan_token(DROP)) return true;
    if (jj_scan_token(NOT)) return true;
    if (jj_scan_token(NULL_)) return true;
    return false;
  }

 inline bool jj_3R_set_column_not_null_clause_4873_5_580()
 {
    if (jj_done) return true;
    if (jj_scan_token(SET)) return true;
    if (jj_scan_token(NOT)) return true;
    if (jj_scan_token(NULL_)) return true;
    return false;
  }

 inline bool jj_3R_drop_column_default_clause_4867_5_579()
 {
    if (jj_done) return true;
    if (jj_scan_token(DROP)) return true;
    if (jj_scan_token(DEFAULT_)) return true;
    return false;
  }

 inline bool jj_3R_set_column_default_clause_4861_5_578()
 {
    if (jj_done) return true;
    if (jj_scan_token(SET)) return true;
    if (jj_3R_default_clause_4682_5_540()) return true;
    return false;
  }

 inline bool jj_3_1427()
 {
    if (jj_done) return true;
    if (jj_3R_drop_column_generation_expression_clause_4929_5_587()) return true;
    return false;
  }

 inline bool jj_3_1426()
 {
    if (jj_done) return true;
    if (jj_3R_drop_identity_property_clause_4923_5_586()) return true;
    return false;
  }

 inline bool jj_3_1425()
 {
    if (jj_done) return true;
    if (jj_3R_alter_identity_column_specification_4903_5_585()) return true;
    return false;
  }

 inline bool jj_3_1424()
 {
    if (jj_done) return true;
    if (jj_3R_alter_column_data_type_clause_4897_5_584()) return true;
    return false;
  }

 inline bool jj_3_1423()
 {
    if (jj_done) return true;
    if (jj_3R_drop_column_scope_clause_4891_5_583()) return true;
    return false;
  }

 inline bool jj_3_1417()
 {
    if (jj_done) return true;
    if (jj_scan_token(COLUMN)) return true;
    return false;
  }

 inline bool jj_3_1422()
 {
    if (jj_done) return true;
    if (jj_3R_add_column_scope_clause_4885_5_582()) return true;
    return false;
  }

 inline bool jj_3_1421()
 {
    if (jj_done) return true;
    if (jj_3R_drop_column_not_null_clause_4879_5_581()) return true;
    return false;
  }

 inline bool jj_3_1420()
 {
    if (jj_done) return true;
    if (jj_3R_set_column_not_null_clause_4873_5_580()) return true;
    return false;
  }

 inline bool jj_3_1419()
 {
    if (jj_done) return true;
    if (jj_3R_drop_column_default_clause_4867_5_579()) return true;
    return false;
  }

 inline bool jj_3R_alter_column_action_4846_5_966()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1418()) {
    jj_scanpos = xsp;
    if (jj_3_1419()) {
    jj_scanpos = xsp;
    if (jj_3_1420()) {
    jj_scanpos = xsp;
    if (jj_3_1421()) {
    jj_scanpos = xsp;
    if (jj_3_1422()) {
    jj_scanpos = xsp;
    if (jj_3_1423()) {
    jj_scanpos = xsp;
    if (jj_3_1424()) {
    jj_scanpos = xsp;
    if (jj_3_1425()) {
    jj_scanpos = xsp;
    if (jj_3_1426()) {
    jj_scanpos = xsp;
    if (jj_3_1427()) return true;
    }
    }
    }
    }
    }
    }
    }
    }
    }
    return false;
  }

 inline bool jj_3_1418()
 {
    if (jj_done) return true;
    if (jj_3R_set_column_default_clause_4861_5_578()) return true;
    return false;
  }

 inline bool jj_3_1416()
 {
    if (jj_done) return true;
    if (jj_scan_token(COLUMN)) return true;
    return false;
  }

 inline bool jj_3R_alter_column_definition_4840_5_570()
 {
    if (jj_done) return true;
    if (jj_scan_token(ALTER)) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1417()) jj_scanpos = xsp;
    if (jj_3R_identifier_928_3_141()) return true;
    if (jj_3R_alter_column_action_4846_5_966()) return true;
    return false;
  }

 inline bool jj_3R_add_column_definition_4834_5_569()
 {
    if (jj_done) return true;
    if (jj_scan_token(ADD)) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1416()) jj_scanpos = xsp;
    if (jj_3R_column_definition_4603_5_531()) return true;
    return false;
  }

 inline bool jj_3_1415()
 {
    if (jj_done) return true;
    if (jj_3R_drop_system_versioning_clause_4989_5_577()) return true;
    return false;
  }

 inline bool jj_3_1414()
 {
    if (jj_done) return true;
    if (jj_3R_alter_system_versioning_clause_4983_5_576()) return true;
    return false;
  }

 inline bool jj_3_1413()
 {
    if (jj_done) return true;
    if (jj_3R_add_system_versioning_clause_4959_5_575()) return true;
    return false;
  }

 inline bool jj_3_1412()
 {
    if (jj_done) return true;
    if (jj_3R_drop_table_constraint_definition_4953_5_574()) return true;
    return false;
  }

 inline bool jj_3_1411()
 {
    if (jj_done) return true;
    if (jj_3R_alter_table_constraint_definition_4947_5_573()) return true;
    return false;
  }

 inline bool jj_3_1410()
 {
    if (jj_done) return true;
    if (jj_3R_add_table_constraint_definition_4941_5_572()) return true;
    return false;
  }

 inline bool jj_3_1409()
 {
    if (jj_done) return true;
    if (jj_3R_drop_column_definition_4935_5_571()) return true;
    return false;
  }

 inline bool jj_3_1408()
 {
    if (jj_done) return true;
    if (jj_3R_alter_column_definition_4840_5_570()) return true;
    return false;
  }

 inline bool jj_3_1407()
 {
    if (jj_done) return true;
    if (jj_3R_add_column_definition_4834_5_569()) return true;
    return false;
  }

 inline bool jj_3R_alter_table_statement_4814_5_742()
 {
    if (jj_done) return true;
    if (jj_scan_token(ALTER)) return true;
    if (jj_scan_token(TABLE)) return true;
    if (jj_3R_table_name_948_5_382()) return true;
    return false;
  }

 inline bool jj_3R_check_constraint_definition_4808_5_559()
 {
    if (jj_done) return true;
    if (jj_scan_token(CHECK)) return true;
    if (jj_scan_token(lparen)) return true;
    if (jj_3R_search_condition_3868_5_818()) return true;
    return false;
  }

 inline bool jj_3_1406()
 {
    if (jj_done) return true;
    if (jj_scan_token(NO)) return true;
    if (jj_scan_token(ACTION)) return true;
    return false;
  }

 inline bool jj_3_1405()
 {
    if (jj_done) return true;
    if (jj_scan_token(RESTRICT)) return true;
    return false;
  }

 inline bool jj_3_1404()
 {
    if (jj_done) return true;
    if (jj_scan_token(SET)) return true;
    if (jj_scan_token(DEFAULT_)) return true;
    return false;
  }

 inline bool jj_3_1403()
 {
    if (jj_done) return true;
    if (jj_scan_token(SET)) return true;
    if (jj_scan_token(NULL_)) return true;
    return false;
  }

 inline bool jj_3R_referential_action_4798_5_965()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1402()) {
    jj_scanpos = xsp;
    if (jj_3_1403()) {
    jj_scanpos = xsp;
    if (jj_3_1404()) {
    jj_scanpos = xsp;
    if (jj_3_1405()) {
    jj_scanpos = xsp;
    if (jj_3_1406()) return true;
    }
    }
    }
    }
    return false;
  }

 inline bool jj_3_1402()
 {
    if (jj_done) return true;
    if (jj_scan_token(CASCADE)) return true;
    return false;
  }

 inline bool jj_3_1399()
 {
    if (jj_done) return true;
    if (jj_3R_update_rule_4786_5_568()) return true;
    return false;
  }

 inline bool jj_3_1398()
 {
    if (jj_done) return true;
    if (jj_3R_delete_rule_4792_5_567()) return true;
    return false;
  }

 inline bool jj_3R_delete_rule_4792_5_567()
 {
    if (jj_done) return true;
    if (jj_scan_token(ON)) return true;
    if (jj_scan_token(DELETE)) return true;
    if (jj_3R_referential_action_4798_5_965()) return true;
    return false;
  }

 inline bool jj_3R_update_rule_4786_5_568()
 {
    if (jj_done) return true;
    if (jj_scan_token(ON)) return true;
    if (jj_scan_token(UPDATE)) return true;
    if (jj_3R_referential_action_4798_5_965()) return true;
    return false;
  }

 inline bool jj_3_1397()
 {
    if (jj_done) return true;
    if (jj_scan_token(lparen)) return true;
    if (jj_3R_reference_column_list_4773_5_566()) return true;
    if (jj_scan_token(rparen)) return true;
    return false;
  }

 inline bool jj_3_1401()
 {
    if (jj_done) return true;
    if (jj_3R_delete_rule_4792_5_567()) return true;
    return false;
  }

 inline bool jj_3R_referential_triggered_action_4779_5_565()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1400()) {
    jj_scanpos = xsp;
    if (jj_3_1401()) return true;
    }
    return false;
  }

 inline bool jj_3_1400()
 {
    if (jj_done) return true;
    if (jj_3R_update_rule_4786_5_568()) return true;
    return false;
  }

 inline bool jj_3_1393()
 {
    if (jj_done) return true;
    if (jj_3R_referential_triggered_action_4779_5_565()) return true;
    return false;
  }

 inline bool jj_3R_reference_column_list_4773_5_566()
 {
    if (jj_done) return true;
    if (jj_3R_column_name_list_2942_5_191()) return true;
    return false;
  }

 inline bool jj_3R_referenced_table_and_columns_4767_5_964()
 {
    if (jj_done) return true;
    if (jj_3R_table_name_948_5_382()) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1397()) jj_scanpos = xsp;
    return false;
  }

 inline bool jj_3_1396()
 {
    if (jj_done) return true;
    if (jj_scan_token(SIMPLE)) return true;
    return false;
  }

 inline bool jj_3_1395()
 {
    if (jj_done) return true;
    if (jj_scan_token(PARTIAL)) return true;
    return false;
  }

 inline bool jj_3R_match_type_4753_5_564()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1394()) {
    jj_scanpos = xsp;
    if (jj_3_1395()) {
    jj_scanpos = xsp;
    if (jj_3_1396()) return true;
    }
    }
    return false;
  }

 inline bool jj_3_1394()
 {
    if (jj_done) return true;
    if (jj_scan_token(FULL)) return true;
    return false;
  }

 inline bool jj_3_1392()
 {
    if (jj_done) return true;
    if (jj_scan_token(MATCH)) return true;
    if (jj_3R_match_type_4753_5_564()) return true;
    return false;
  }

 inline bool jj_3R_references_specification_4746_5_558()
 {
    if (jj_done) return true;
    if (jj_scan_token(REFERENCES)) return true;
    if (jj_3R_referenced_table_and_columns_4767_5_964()) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1392()) jj_scanpos = xsp;
    xsp = jj_scanpos;
    if (jj_3_1393()) jj_scanpos = xsp;
    return false;
  }

 inline bool jj_3R_referential_constraint_definition_4739_5_562()
 {
    if (jj_done) return true;
    if (jj_scan_token(FOREIGN)) return true;
    if (jj_scan_token(KEY)) return true;
    if (jj_scan_token(lparen)) return true;
    return false;
  }

 inline bool jj_3R_unique_column_list_4733_5_563()
 {
    if (jj_done) return true;
    if (jj_3R_column_name_list_2942_5_191()) return true;
    return false;
  }

 inline bool jj_3_1391()
 {
    if (jj_done) return true;
    if (jj_scan_token(PRIMARY)) return true;
    if (jj_scan_token(KEY)) return true;
    return false;
  }

 inline bool jj_3R_unique_specification_4726_5_557()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1390()) {
    jj_scanpos = xsp;
    if (jj_3_1391()) return true;
    }
    return false;
  }

 inline bool jj_3_1390()
 {
    if (jj_done) return true;
    if (jj_scan_token(UNIQUE)) return true;
    return false;
  }

 inline bool jj_3_1389()
 {
    if (jj_done) return true;
    if (jj_scan_token(UNIQUE)) return true;
    if (jj_scan_token(VALUE)) return true;
    return false;
  }

 inline bool jj_3R_unique_constraint_definition_4719_5_561()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1388()) {
    jj_scanpos = xsp;
    if (jj_3_1389()) return true;
    }
    return false;
  }

 inline bool jj_3_1388()
 {
    if (jj_done) return true;
    if (jj_3R_unique_specification_4726_5_557()) return true;
    if (jj_scan_token(lparen)) return true;
    if (jj_3R_unique_column_list_4733_5_563()) return true;
    return false;
  }

 inline bool jj_3_1387()
 {
    if (jj_done) return true;
    if (jj_3R_check_constraint_definition_4808_5_559()) return true;
    return false;
  }

 inline bool jj_3_1386()
 {
    if (jj_done) return true;
    if (jj_3R_referential_constraint_definition_4739_5_562()) return true;
    return false;
  }

 inline bool jj_3R_table_constraint_4711_5_958()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1385()) {
    jj_scanpos = xsp;
    if (jj_3_1386()) {
    jj_scanpos = xsp;
    if (jj_3_1387()) return true;
    }
    }
    return false;
  }

 inline bool jj_3_1385()
 {
    if (jj_done) return true;
    if (jj_3R_unique_constraint_definition_4719_5_561()) return true;
    return false;
  }

 inline bool jj_3_1384()
 {
    if (jj_done) return true;
    if (jj_3R_constraint_characteristics_4088_5_556()) return true;
    return false;
  }

 inline bool jj_3_1383()
 {
    if (jj_done) return true;
    if (jj_3R_constraint_name_definition_4082_5_555()) return true;
    return false;
  }

 inline bool jj_3R_table_constraint_definition_4704_5_532()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1383()) jj_scanpos = xsp;
    if (jj_3R_table_constraint_4711_5_958()) return true;
    xsp = jj_scanpos;
    if (jj_3_1384()) jj_scanpos = xsp;
    return false;
  }

 inline bool jj_3_1382()
 {
    if (jj_done) return true;
    if (jj_3R_implicitly_typed_value_specification_1482_5_209()) return true;
    return false;
  }

 inline bool jj_3_1364()
 {
    if (jj_done) return true;
    if (jj_3R_constraint_characteristics_4088_5_556()) return true;
    return false;
  }

 inline bool jj_3_1381()
 {
    if (jj_done) return true;
    if (jj_scan_token(CURRENT_PATH)) return true;
    return false;
  }

 inline bool jj_3_1380()
 {
    if (jj_done) return true;
    if (jj_scan_token(CURRENT_SCHEMA)) return true;
    return false;
  }

 inline bool jj_3_1379()
 {
    if (jj_done) return true;
    if (jj_scan_token(CURRENT_CATALOG)) return true;
    return false;
  }

 inline bool jj_3_1378()
 {
    if (jj_done) return true;
    if (jj_scan_token(SYSTEM_USER)) return true;
    return false;
  }

 inline bool jj_3_1377()
 {
    if (jj_done) return true;
    if (jj_scan_token(SESSION_USER)) return true;
    return false;
  }

 inline bool jj_3_1376()
 {
    if (jj_done) return true;
    if (jj_scan_token(CURRENT_ROLE)) return true;
    return false;
  }

 inline bool jj_3_1375()
 {
    if (jj_done) return true;
    if (jj_scan_token(CURRENT_USER)) return true;
    return false;
  }

 inline bool jj_3_1374()
 {
    if (jj_done) return true;
    if (jj_scan_token(USER)) return true;
    return false;
  }

 inline bool jj_3_1373()
 {
    if (jj_done) return true;
    if (jj_3R_datetime_value_function_2407_3_324()) return true;
    return false;
  }

 inline bool jj_3R_default_option_4688_5_960()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1372()) {
    jj_scanpos = xsp;
    if (jj_3_1373()) {
    jj_scanpos = xsp;
    if (jj_3_1374()) {
    jj_scanpos = xsp;
    if (jj_3_1375()) {
    jj_scanpos = xsp;
    if (jj_3_1376()) {
    jj_scanpos = xsp;
    if (jj_3_1377()) {
    jj_scanpos = xsp;
    if (jj_3_1378()) {
    jj_scanpos = xsp;
    if (jj_3_1379()) {
    jj_scanpos = xsp;
    if (jj_3_1380()) {
    jj_scanpos = xsp;
    if (jj_3_1381()) {
    jj_scanpos = xsp;
    if (jj_3_1382()) return true;
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    return false;
  }

 inline bool jj_3_1372()
 {
    if (jj_done) return true;
    if (jj_3R_literal_818_5_203()) return true;
    return false;
  }

 inline bool jj_3R_default_clause_4682_5_540()
 {
    if (jj_done) return true;
    if (jj_scan_token(DEFAULT_)) return true;
    if (jj_3R_default_option_4688_5_960()) return true;
    return false;
  }

 inline bool jj_3_1370()
 {
    if (jj_done) return true;
    if (jj_scan_token(BY)) return true;
    if (jj_scan_token(DEFAULT_)) return true;
    return false;
  }

 inline bool jj_3_1369()
 {
    if (jj_done) return true;
    if (jj_scan_token(ALWAYS)) return true;
    return false;
  }

 inline bool jj_3R_generation_rule_4670_5_962()
 {
    if (jj_done) return true;
    if (jj_scan_token(GENERATED)) return true;
    if (jj_scan_token(ALWAYS)) return true;
    return false;
  }

 inline bool jj_3R_generation_clause_4664_5_551()
 {
    if (jj_done) return true;
    if (jj_3R_generation_rule_4670_5_962()) return true;
    if (jj_scan_token(AS)) return true;
    return false;
  }

 inline bool jj_3_1354()
 {
    if (jj_done) return true;
    if (jj_3R_generation_clause_4664_5_551()) return true;
    return false;
  }

 inline bool jj_3_1371()
 {
    if (jj_done) return true;
    if (jj_scan_token(lparen)) return true;
    if (jj_3R_common_sequence_generator_options_6144_5_560()) return true;
    if (jj_scan_token(rparen)) return true;
    return false;
  }

 inline bool jj_3R_identity_column_specification_4657_5_550()
 {
    if (jj_done) return true;
    if (jj_scan_token(GENERATED)) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1369()) {
    jj_scanpos = xsp;
    if (jj_3_1370()) return true;
    }
    if (jj_scan_token(AS)) return true;
    return false;
  }

 inline bool jj_3_1356()
 {
    if (jj_done) return true;
    if (jj_3R_system_version_end_column_specification_4630_5_553()) return true;
    return false;
  }

 inline bool jj_3_1368()
 {
    if (jj_done) return true;
    if (jj_3R_check_constraint_definition_4808_5_559()) return true;
    return false;
  }

 inline bool jj_3_1367()
 {
    if (jj_done) return true;
    if (jj_3R_references_specification_4746_5_558()) return true;
    return false;
  }

 inline bool jj_3_1366()
 {
    if (jj_done) return true;
    if (jj_3R_unique_specification_4726_5_557()) return true;
    return false;
  }

 inline bool jj_3R_column_constraint_4648_5_961()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1365()) {
    jj_scanpos = xsp;
    if (jj_3_1366()) {
    jj_scanpos = xsp;
    if (jj_3_1367()) {
    jj_scanpos = xsp;
    if (jj_3_1368()) return true;
    }
    }
    }
    return false;
  }

 inline bool jj_3_1365()
 {
    if (jj_done) return true;
    if (jj_scan_token(NOT)) return true;
    if (jj_scan_token(NULL_)) return true;
    return false;
  }

 inline bool jj_3_1363()
 {
    if (jj_done) return true;
    if (jj_3R_constraint_name_definition_4082_5_555()) return true;
    return false;
  }

 inline bool jj_3R_column_constraint_definition_4642_5_541()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1363()) jj_scanpos = xsp;
    if (jj_3R_column_constraint_4648_5_961()) return true;
    xsp = jj_scanpos;
    if (jj_3_1364()) jj_scanpos = xsp;
    return false;
  }

 inline bool jj_3R_timestamp_generation_rule_4636_5_963()
 {
    if (jj_done) return true;
    if (jj_scan_token(GENERATED)) return true;
    if (jj_scan_token(ALWAYS)) return true;
    return false;
  }

 inline bool jj_3R_system_version_end_column_specification_4630_5_553()
 {
    if (jj_done) return true;
    if (jj_3R_timestamp_generation_rule_4636_5_963()) return true;
    if (jj_scan_token(AS)) return true;
    return false;
  }

 inline bool jj_3_1353()
 {
    if (jj_done) return true;
    if (jj_3R_identity_column_specification_4657_5_550()) return true;
    return false;
  }

 inline bool jj_3R_system_version_start_column_specification_4624_5_552()
 {
    if (jj_done) return true;
    if (jj_3R_timestamp_generation_rule_4636_5_963()) return true;
    if (jj_scan_token(AS)) return true;
    return false;
  }

 inline bool jj_3_1362()
 {
    if (jj_done) return true;
    if (jj_3R_schema_qualified_name_970_5_252()) return true;
    return false;
  }

 inline bool jj_3R_data_type_or_schema_qualified_name_4617_5_549()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1361()) {
    jj_scanpos = xsp;
    if (jj_3_1362()) return true;
    }
    return false;
  }

 inline bool jj_3_1361()
 {
    if (jj_done) return true;
    if (jj_3R_data_type_1086_3_251()) return true;
    return false;
  }

 inline bool jj_3_1360()
 {
    if (jj_done) return true;
    if (jj_3R_column_description_8015_5_554()) return true;
    return false;
  }

 inline bool jj_3_1359()
 {
    if (jj_done) return true;
    if (jj_3R_collate_clause_4076_5_153()) return true;
    return false;
  }

 inline bool jj_3_1358()
 {
    if (jj_done) return true;
    if (jj_3R_column_constraint_definition_4642_5_541()) return true;
    return false;
  }

 inline bool jj_3_1355()
 {
    if (jj_done) return true;
    if (jj_3R_system_version_start_column_specification_4624_5_552()) return true;
    return false;
  }

 inline bool jj_3_1352()
 {
    if (jj_done) return true;
    if (jj_3R_default_clause_4682_5_540()) return true;
    return false;
  }

 inline bool jj_3_1357()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1352()) {
    jj_scanpos = xsp;
    if (jj_3_1353()) {
    jj_scanpos = xsp;
    if (jj_3_1354()) {
    jj_scanpos = xsp;
    if (jj_3_1355()) {
    jj_scanpos = xsp;
    if (jj_3_1356()) return true;
    }
    }
    }
    }
    return false;
  }

 inline bool jj_3_1351()
 {
    if (jj_done) return true;
    if (jj_3R_data_type_or_schema_qualified_name_4617_5_549()) return true;
    return false;
  }

 inline bool jj_3R_column_definition_4603_5_531()
 {
    if (jj_done) return true;
    if (jj_3R_identifier_928_3_141()) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1351()) jj_scanpos = xsp;
    xsp = jj_scanpos;
    if (jj_3_1357()) jj_scanpos = xsp;
    while (true) {
      xsp = jj_scanpos;
      if (jj_3_1358()) { jj_scanpos = xsp; break; }
    }
    xsp = jj_scanpos;
    if (jj_3_1359()) jj_scanpos = xsp;
    xsp = jj_scanpos;
    if (jj_3_1360()) jj_scanpos = xsp;
    return false;
  }

 inline bool jj_3_1350()
 {
    if (jj_done) return true;
    if (jj_scan_token(WITH)) return true;
    if (jj_scan_token(DATA)) return true;
    return false;
  }

 inline bool jj_3R_with_or_without_data_4596_5_548()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1349()) {
    jj_scanpos = xsp;
    if (jj_3_1350()) return true;
    }
    return false;
  }

 inline bool jj_3_1349()
 {
    if (jj_done) return true;
    if (jj_scan_token(WITH)) return true;
    if (jj_scan_token(NO)) return true;
    if (jj_scan_token(DATA)) return true;
    return false;
  }

 inline bool jj_3_1348()
 {
    if (jj_done) return true;
    if (jj_3R_with_or_without_data_4596_5_548()) return true;
    return false;
  }

 inline bool jj_3_1347()
 {
    if (jj_done) return true;
    if (jj_3R_query_expression_3399_5_547()) return true;
    return false;
  }

 inline bool jj_3_1346()
 {
    if (jj_done) return true;
    if (jj_3R_subquery_3527_5_181()) return true;
    return false;
  }

 inline bool jj_3_1345()
 {
    if (jj_done) return true;
    if (jj_scan_token(WITH)) return true;
    if (jj_3R_table_attributes_8051_5_523()) return true;
    return false;
  }

 inline bool jj_3_1344()
 {
    if (jj_done) return true;
    if (jj_scan_token(lparen)) return true;
    if (jj_3R_column_name_list_2942_5_191()) return true;
    if (jj_scan_token(rparen)) return true;
    return false;
  }

 inline bool jj_3R_as_subquery_clause_4579_5_527()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1344()) jj_scanpos = xsp;
    xsp = jj_scanpos;
    if (jj_3_1345()) jj_scanpos = xsp;
    if (jj_scan_token(AS)) return true;
    xsp = jj_scanpos;
    if (jj_3_1346()) {
    jj_scanpos = xsp;
    if (jj_3_1347()) return true;
    }
    return false;
  }

 inline bool jj_3_1343()
 {
    if (jj_done) return true;
    if (jj_scan_token(EXCLUDING)) return true;
    if (jj_scan_token(GENERATED)) return true;
    return false;
  }

 inline bool jj_3R_generation_option_4572_5_546()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1342()) {
    jj_scanpos = xsp;
    if (jj_3_1343()) return true;
    }
    return false;
  }

 inline bool jj_3_1342()
 {
    if (jj_done) return true;
    if (jj_scan_token(INCLUDING)) return true;
    if (jj_scan_token(GENERATED)) return true;
    return false;
  }

 inline bool jj_3_1341()
 {
    if (jj_done) return true;
    if (jj_scan_token(EXCLUDING)) return true;
    if (jj_scan_token(DEFAULTS)) return true;
    return false;
  }

 inline bool jj_3R_column_default_option_4565_5_545()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1340()) {
    jj_scanpos = xsp;
    if (jj_3_1341()) return true;
    }
    return false;
  }

 inline bool jj_3_1340()
 {
    if (jj_done) return true;
    if (jj_scan_token(INCLUDING)) return true;
    if (jj_scan_token(DEFAULTS)) return true;
    return false;
  }

 inline bool jj_3_1332()
 {
    if (jj_done) return true;
    if (jj_3R_like_options_4543_5_542()) return true;
    return false;
  }

 inline bool jj_3_1339()
 {
    if (jj_done) return true;
    if (jj_scan_token(EXCLUDING)) return true;
    if (jj_scan_token(IDENTITY)) return true;
    return false;
  }

 inline bool jj_3R_identity_option_4558_5_544()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1338()) {
    jj_scanpos = xsp;
    if (jj_3_1339()) return true;
    }
    return false;
  }

 inline bool jj_3_1338()
 {
    if (jj_done) return true;
    if (jj_scan_token(INCLUDING)) return true;
    if (jj_scan_token(IDENTITY)) return true;
    return false;
  }

 inline bool jj_3_1331()
 {
    if (jj_done) return true;
    if (jj_3R_column_constraint_definition_4642_5_541()) return true;
    return false;
  }

 inline bool jj_3_1337()
 {
    if (jj_done) return true;
    if (jj_scan_token(INCLUDING)) return true;
    if (jj_scan_token(PROPERTIES)) return true;
    return false;
  }

 inline bool jj_3_1336()
 {
    if (jj_done) return true;
    if (jj_3R_generation_option_4572_5_546()) return true;
    return false;
  }

 inline bool jj_3_1335()
 {
    if (jj_done) return true;
    if (jj_3R_column_default_option_4565_5_545()) return true;
    return false;
  }

 inline bool jj_3R_like_option_4549_5_543()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1334()) {
    jj_scanpos = xsp;
    if (jj_3_1335()) {
    jj_scanpos = xsp;
    if (jj_3_1336()) {
    jj_scanpos = xsp;
    if (jj_3_1337()) return true;
    }
    }
    }
    return false;
  }

 inline bool jj_3_1334()
 {
    if (jj_done) return true;
    if (jj_3R_identity_option_4558_5_544()) return true;
    return false;
  }

 inline bool jj_3_1312()
 {
    if (jj_done) return true;
    if (jj_scan_token(YEARS)) return true;
    return false;
  }

 inline bool jj_3_1333()
 {
    if (jj_done) return true;
    if (jj_3R_like_option_4549_5_543()) return true;
    return false;
  }

 inline bool jj_3R_like_options_4543_5_542()
 {
    if (jj_done) return true;
    Token * xsp;
    if (jj_3_1333()) return true;
    while (true) {
      xsp = jj_scanpos;
      if (jj_3_1333()) { jj_scanpos = xsp; break; }
    }
    return false;
  }

 inline bool jj_3R_like_clause_4537_5_533()
 {
    if (jj_done) return true;
    if (jj_scan_token(LIKE)) return true;
    if (jj_3R_table_name_948_5_382()) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1332()) jj_scanpos = xsp;
    return false;
  }

 inline bool jj_3_1311()
 {
    if (jj_done) return true;
    if (jj_scan_token(YEAR)) return true;
    return false;
  }

 inline bool jj_3_1330()
 {
    if (jj_done) return true;
    if (jj_3R_default_clause_4682_5_540()) return true;
    return false;
  }

 inline bool jj_3R_supertable_name_4531_5_1032()
 {
    if (jj_done) return true;
    if (jj_3R_table_name_948_5_382()) return true;
    return false;
  }

 inline bool jj_3_1310()
 {
    if (jj_done) return true;
    if (jj_scan_token(MONTHS)) return true;
    return false;
  }

 inline bool jj_3R_supertable_clause_4525_5_959()
 {
    if (jj_done) return true;
    if (jj_3R_supertable_name_4531_5_1032()) return true;
    return false;
  }

 inline bool jj_3R_subtable_clause_4519_5_534()
 {
    if (jj_done) return true;
    if (jj_scan_token(UNDER)) return true;
    if (jj_3R_supertable_clause_4525_5_959()) return true;
    return false;
  }

 inline bool jj_3_1325()
 {
    if (jj_done) return true;
    if (jj_3R_reference_generation_4499_5_539()) return true;
    return false;
  }

 inline bool jj_3_1309()
 {
    if (jj_done) return true;
    if (jj_scan_token(MONTH)) return true;
    return false;
  }

 inline bool jj_3_1319()
 {
    if (jj_done) return true;
    if (jj_3R_subtable_clause_4519_5_534()) return true;
    return false;
  }

 inline bool jj_3_1329()
 {
    if (jj_done) return true;
    if (jj_3R_scope_clause_1269_5_171()) return true;
    return false;
  }

 inline bool jj_3_1308()
 {
    if (jj_done) return true;
    if (jj_scan_token(DAYS)) return true;
    return false;
  }

 inline bool jj_3R_column_options_4507_5_537()
 {
    if (jj_done) return true;
    if (jj_3R_identifier_928_3_141()) return true;
    if (jj_scan_token(WITH)) return true;
    if (jj_scan_token(OPTIONS)) return true;
    return false;
  }

 inline bool jj_3_1307()
 {
    if (jj_done) return true;
    if (jj_scan_token(DAY)) return true;
    return false;
  }

 inline bool jj_3_1328()
 {
    if (jj_done) return true;
    if (jj_scan_token(DERIVED)) return true;
    return false;
  }

 inline bool jj_3_1327()
 {
    if (jj_done) return true;
    if (jj_scan_token(USER)) return true;
    if (jj_scan_token(GENERATED)) return true;
    return false;
  }

 inline bool jj_3R_reference_generation_4499_5_539()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1326()) {
    jj_scanpos = xsp;
    if (jj_3_1327()) {
    jj_scanpos = xsp;
    if (jj_3_1328()) return true;
    }
    }
    return false;
  }

 inline bool jj_3_1326()
 {
    if (jj_done) return true;
    if (jj_scan_token(SYSTEM)) return true;
    if (jj_scan_token(GENERATED)) return true;
    return false;
  }

 inline bool jj_3_1306()
 {
    if (jj_done) return true;
    if (jj_scan_token(HOURS)) return true;
    return false;
  }

 inline bool jj_3R_self_referencing_column_specification_4493_5_538()
 {
    if (jj_done) return true;
    if (jj_scan_token(REF)) return true;
    if (jj_scan_token(IS)) return true;
    if (jj_3R_identifier_928_3_141()) return true;
    return false;
  }

 inline bool jj_3_1324()
 {
    if (jj_done) return true;
    if (jj_3R_self_referencing_column_specification_4493_5_538()) return true;
    return false;
  }

 inline bool jj_3_1305()
 {
    if (jj_done) return true;
    if (jj_scan_token(HOUR)) return true;
    return false;
  }

 inline bool jj_3_1323()
 {
    if (jj_done) return true;
    if (jj_3R_table_constraint_definition_4704_5_532()) return true;
    return false;
  }

 inline bool jj_3R_typed_table_element_4485_5_536()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1322()) {
    jj_scanpos = xsp;
    if (jj_3_1323()) {
    jj_scanpos = xsp;
    if (jj_3_1324()) return true;
    }
    }
    return false;
  }

 inline bool jj_3_1322()
 {
    if (jj_done) return true;
    if (jj_3R_column_options_4507_5_537()) return true;
    return false;
  }

 inline bool jj_3_1321()
 {
    if (jj_done) return true;
    if (jj_scan_token(570)) return true;
    if (jj_3R_typed_table_element_4485_5_536()) return true;
    return false;
  }

 inline bool jj_3_1315()
 {
    if (jj_done) return true;
    if (jj_scan_token(570)) return true;
    if (jj_3R_table_element_4463_5_530()) return true;
    return false;
  }

 inline bool jj_3R_typed_table_element_list_4478_6_535()
 {
    if (jj_done) return true;
    if (jj_scan_token(lparen)) return true;
    if (jj_3R_typed_table_element_4485_5_536()) return true;
    return false;
  }

 inline bool jj_3_1304()
 {
    if (jj_done) return true;
    if (jj_scan_token(MINUTES)) return true;
    return false;
  }

 inline bool jj_3_1320()
 {
    if (jj_done) return true;
    if (jj_3R_typed_table_element_list_4478_6_535()) return true;
    return false;
  }

 inline bool jj_3R_typed_table_clause_4471_5_526()
 {
    if (jj_done) return true;
    if (jj_scan_token(OF)) return true;
    if (jj_3R_path_resolved_user_defined_type_name_1281_5_151()) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1319()) jj_scanpos = xsp;
    xsp = jj_scanpos;
    if (jj_3_1320()) jj_scanpos = xsp;
    return false;
  }

 inline bool jj_3_1303()
 {
    if (jj_done) return true;
    if (jj_scan_token(MINUTE)) return true;
    return false;
  }

 inline bool jj_3_1318()
 {
    if (jj_done) return true;
    if (jj_3R_like_clause_4537_5_533()) return true;
    return false;
  }

 inline bool jj_3_1317()
 {
    if (jj_done) return true;
    if (jj_3R_table_constraint_definition_4704_5_532()) return true;
    return false;
  }

 inline bool jj_3R_table_element_4463_5_530()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1316()) {
    jj_scanpos = xsp;
    if (jj_3_1317()) {
    jj_scanpos = xsp;
    if (jj_3_1318()) return true;
    }
    }
    return false;
  }

 inline bool jj_3_1316()
 {
    if (jj_done) return true;
    if (jj_3R_column_definition_4603_5_531()) return true;
    return false;
  }

 inline bool jj_3R_table_element_list_4457_6_528()
 {
    if (jj_done) return true;
    if (jj_scan_token(lparen)) return true;
    if (jj_3R_table_element_4463_5_530()) return true;
    Token * xsp;
    while (true) {
      xsp = jj_scanpos;
      if (jj_3_1315()) { jj_scanpos = xsp; break; }
    }
    if (jj_scan_token(rparen)) return true;
    return false;
  }

 inline bool jj_3_1302()
 {
    if (jj_done) return true;
    if (jj_scan_token(SECONDS)) return true;
    return false;
  }

 inline bool jj_3_1314()
 {
    if (jj_done) return true;
    if (jj_scan_token(DELETE)) return true;
    return false;
  }

 inline bool jj_3R_table_commit_action_4450_5_525()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1313()) {
    jj_scanpos = xsp;
    if (jj_3_1314()) return true;
    }
    return false;
  }

 inline bool jj_3_1313()
 {
    if (jj_done) return true;
    if (jj_scan_token(PRESERVE)) return true;
    return false;
  }

 inline bool jj_3_1298()
 {
    if (jj_done) return true;
    if (jj_3R_retention_period_specification_4431_5_529()) return true;
    return false;
  }

 inline bool jj_3_1301()
 {
    if (jj_done) return true;
    if (jj_scan_token(SECOND)) return true;
    return false;
  }

 inline bool jj_3_1300()
 {
    if (jj_done) return true;
    if (jj_scan_token(KEEP)) return true;
    if (jj_scan_token(VERSIONS)) return true;
    if (jj_scan_token(FOR)) return true;
    return false;
  }

 inline bool jj_3R_retention_period_specification_4431_5_529()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1299()) {
    jj_scanpos = xsp;
    if (jj_3_1300()) return true;
    }
    return false;
  }

 inline bool jj_3_1299()
 {
    if (jj_done) return true;
    if (jj_scan_token(KEEP)) return true;
    if (jj_scan_token(VERSIONS)) return true;
    if (jj_scan_token(FOREVER)) return true;
    return false;
  }

 inline bool jj_3R_system_versioning_clause_4425_5_522()
 {
    if (jj_done) return true;
    if (jj_scan_token(SYSTEM)) return true;
    if (jj_scan_token(VERSIONING)) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1298()) jj_scanpos = xsp;
    return false;
  }

 inline bool jj_3_1297()
 {
    if (jj_done) return true;
    if (jj_scan_token(LOCAL)) return true;
    return false;
  }

 inline bool jj_3R_global_or_local_4418_5_957()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1296()) {
    jj_scanpos = xsp;
    if (jj_3_1297()) return true;
    }
    return false;
  }

 inline bool jj_3_1296()
 {
    if (jj_done) return true;
    if (jj_scan_token(GLOBAL)) return true;
    return false;
  }

 inline bool jj_3R_table_scope_4412_5_520()
 {
    if (jj_done) return true;
    if (jj_3R_global_or_local_4418_5_957()) return true;
    if (jj_scan_token(TEMPORARY)) return true;
    return false;
  }

 inline bool jj_3_1295()
 {
    if (jj_done) return true;
    if (jj_3R_table_element_list_4457_6_528()) return true;
    return false;
  }

 inline bool jj_3_1294()
 {
    if (jj_done) return true;
    if (jj_3R_as_subquery_clause_4579_5_527()) return true;
    return false;
  }

 inline bool jj_3R_table_contents_source_4404_5_524()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1293()) {
    jj_scanpos = xsp;
    if (jj_3_1294()) {
    jj_scanpos = xsp;
    if (jj_3_1295()) return true;
    }
    }
    return false;
  }

 inline bool jj_3_1293()
 {
    if (jj_done) return true;
    if (jj_3R_typed_table_clause_4471_5_526()) return true;
    return false;
  }

 inline bool jj_3_1288()
 {
    if (jj_done) return true;
    if (jj_3R_table_attributes_8051_5_523()) return true;
    return false;
  }

 inline bool jj_3_1292()
 {
    if (jj_done) return true;
    if (jj_scan_token(ON)) return true;
    if (jj_scan_token(COMMIT)) return true;
    if (jj_3R_table_commit_action_4450_5_525()) return true;
    return false;
  }

 inline bool jj_3_1287()
 {
    if (jj_done) return true;
    if (jj_3R_system_versioning_clause_4425_5_522()) return true;
    return false;
  }

 inline bool jj_3_1289()
 {
    if (jj_done) return true;
    if (jj_scan_token(WITH)) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1287()) {
    jj_scanpos = xsp;
    if (jj_3_1288()) return true;
    }
    return false;
  }

 inline bool jj_3_1286()
 {
    if (jj_done) return true;
    if (jj_3R_table_description_8003_5_521()) return true;
    return false;
  }

 inline bool jj_3_1290()
 {
    if (jj_done) return true;
    if (jj_scan_token(WITH)) return true;
    return false;
  }

 inline bool jj_3_1291()
 {
    if (jj_done) return true;
    if (jj_3R_table_contents_source_4404_5_524()) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1286()) jj_scanpos = xsp;
    xsp = jj_scanpos;
    if (jj_3_1289()) jj_scanpos = xsp;
    return false;
  }

 inline bool jj_3_1284()
 {
    if (jj_done) return true;
    if (jj_3R_table_attributes_8051_5_523()) return true;
    return false;
  }

 inline bool jj_3_1283()
 {
    if (jj_done) return true;
    if (jj_3R_system_versioning_clause_4425_5_522()) return true;
    return false;
  }

 inline bool jj_3_1285()
 {
    if (jj_done) return true;
    if (jj_scan_token(WITH)) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1283()) {
    jj_scanpos = xsp;
    if (jj_3_1284()) return true;
    }
    return false;
  }

 inline bool jj_3_1280()
 {
    if (jj_done) return true;
    if (jj_3R_table_scope_4412_5_520()) return true;
    return false;
  }

 inline bool jj_3_1282()
 {
    if (jj_done) return true;
    if (jj_3R_table_description_8003_5_521()) return true;
    return false;
  }

 inline bool jj_3_1281()
 {
    if (jj_done) return true;
    if (jj_3R_if_not_exists_7938_5_499()) return true;
    return false;
  }

 inline bool jj_3R_table_definition_4373_5_504()
 {
    if (jj_done) return true;
    if (jj_scan_token(CREATE)) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1280()) jj_scanpos = xsp;
    if (jj_scan_token(TABLE)) return true;
    xsp = jj_scanpos;
    if (jj_3_1281()) jj_scanpos = xsp;
    if (jj_3R_table_name_948_5_382()) return true;
    return false;
  }

 inline bool jj_3_1279()
 {
    if (jj_done) return true;
    if (jj_scan_token(RESTRICT)) return true;
    return false;
  }

 inline bool jj_3R_drop_behavior_4366_5_592()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1278()) {
    jj_scanpos = xsp;
    if (jj_3_1279()) return true;
    }
    return false;
  }

 inline bool jj_3_1278()
 {
    if (jj_done) return true;
    if (jj_scan_token(CASCADE)) return true;
    return false;
  }

 inline bool jj_3R_drop_schema_statement_4360_5_741()
 {
    if (jj_done) return true;
    if (jj_scan_token(DROP)) return true;
    if (jj_scan_token(SCHEMA)) return true;
    if (jj_3R_schema_name_956_5_140()) return true;
    return false;
  }

 inline bool jj_3_1277()
 {
    if (jj_done) return true;
    if (jj_3R_role_definition_6350_5_519()) return true;
    return false;
  }

 inline bool jj_3_1276()
 {
    if (jj_done) return true;
    if (jj_3R_grant_statement_6266_5_518()) return true;
    return false;
  }

 inline bool jj_3_1275()
 {
    if (jj_done) return true;
    if (jj_3R_sequence_generator_definition_6125_5_517()) return true;
    return false;
  }

 inline bool jj_3_1274()
 {
    if (jj_done) return true;
    if (jj_3R_schema_routine_5576_5_516()) return true;
    return false;
  }

 inline bool jj_3_1273()
 {
    if (jj_done) return true;
    if (jj_3R_transform_definition_6009_5_515()) return true;
    return false;
  }

 inline bool jj_3_1272()
 {
    if (jj_done) return true;
    if (jj_3R_user_defined_ordering_definition_5940_5_514()) return true;
    return false;
  }

 inline bool jj_3_1271()
 {
    if (jj_done) return true;
    if (jj_3R_user_defined_cast_definition_5907_5_513()) return true;
    return false;
  }

 inline bool jj_3_1270()
 {
    if (jj_done) return true;
    if (jj_3R_user_defined_type_definition_5302_5_512()) return true;
    return false;
  }

 inline bool jj_3_1269()
 {
    if (jj_done) return true;
    if (jj_3R_trigger_definition_5230_5_511()) return true;
    return false;
  }

 inline bool jj_3_1268()
 {
    if (jj_done) return true;
    if (jj_3R_assertion_definition_5216_5_510()) return true;
    return false;
  }

 inline bool jj_3_1267()
 {
    if (jj_done) return true;
    if (jj_3R_transliteration_definition_5177_5_509()) return true;
    return false;
  }

 inline bool jj_3_1266()
 {
    if (jj_done) return true;
    if (jj_3R_collation_definition_5157_5_508()) return true;
    return false;
  }

 inline bool jj_3_1265()
 {
    if (jj_done) return true;
    if (jj_3R_character_set_definition_5138_5_507()) return true;
    return false;
  }

 inline bool jj_3_1264()
 {
    if (jj_done) return true;
    if (jj_3R_domain_definition_5077_5_506()) return true;
    return false;
  }

 inline bool jj_3_1263()
 {
    if (jj_done) return true;
    if (jj_3R_view_definition_5004_5_505()) return true;
    return false;
  }

 inline bool jj_3R_schema_element_4339_5_501()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1262()) {
    jj_scanpos = xsp;
    if (jj_3_1263()) {
    jj_scanpos = xsp;
    if (jj_3_1264()) {
    jj_scanpos = xsp;
    if (jj_3_1265()) {
    jj_scanpos = xsp;
    if (jj_3_1266()) {
    jj_scanpos = xsp;
    if (jj_3_1267()) {
    jj_scanpos = xsp;
    if (jj_3_1268()) {
    jj_scanpos = xsp;
    if (jj_3_1269()) {
    jj_scanpos = xsp;
    if (jj_3_1270()) {
    jj_scanpos = xsp;
    if (jj_3_1271()) {
    jj_scanpos = xsp;
    if (jj_3_1272()) {
    jj_scanpos = xsp;
    if (jj_3_1273()) {
    jj_scanpos = xsp;
    if (jj_3_1274()) {
    jj_scanpos = xsp;
    if (jj_3_1275()) {
    jj_scanpos = xsp;
    if (jj_3_1276()) {
    jj_scanpos = xsp;
    if (jj_3_1277()) return true;
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    return false;
  }

 inline bool jj_3_1262()
 {
    if (jj_done) return true;
    if (jj_3R_table_definition_4373_5_504()) return true;
    return false;
  }

 inline bool jj_3R_schema_path_specification_4333_5_503()
 {
    if (jj_done) return true;
    if (jj_3R_path_specification_3954_5_954()) return true;
    return false;
  }

 inline bool jj_3R_schema_character_set_specification_4327_5_502()
 {
    if (jj_done) return true;
    if (jj_scan_token(DEFAULT_)) return true;
    if (jj_scan_token(CHARACTER)) return true;
    if (jj_scan_token(SET)) return true;
    return false;
  }

 inline bool jj_3_1261()
 {
    if (jj_done) return true;
    if (jj_3R_schema_name_956_5_140()) return true;
    if (jj_scan_token(AUTHORIZATION)) return true;
    if (jj_3R_identifier_928_3_141()) return true;
    return false;
  }

 inline bool jj_3_1260()
 {
    if (jj_done) return true;
    if (jj_scan_token(AUTHORIZATION)) return true;
    if (jj_3R_identifier_928_3_141()) return true;
    return false;
  }

 inline bool jj_3R_schema_name_clause_4319_5_992()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1259()) {
    jj_scanpos = xsp;
    if (jj_3_1260()) {
    jj_scanpos = xsp;
    if (jj_3_1261()) return true;
    }
    }
    return false;
  }

 inline bool jj_3_1259()
 {
    if (jj_done) return true;
    if (jj_3R_schema_name_956_5_140()) return true;
    return false;
  }

 inline bool jj_3_1247()
 {
    if (jj_done) return true;
    if (jj_3R_null_ordering_4291_5_498()) return true;
    return false;
  }

 inline bool jj_3_1258()
 {
    if (jj_done) return true;
    if (jj_3R_schema_path_specification_4333_5_503()) return true;
    if (jj_3R_schema_character_set_specification_4327_5_502()) return true;
    return false;
  }

 inline bool jj_3_1257()
 {
    if (jj_done) return true;
    if (jj_3R_schema_character_set_specification_4327_5_502()) return true;
    return false;
  }

 inline bool jj_3_1256()
 {
    if (jj_done) return true;
    if (jj_3R_schema_path_specification_4333_5_503()) return true;
    return false;
  }

 inline bool jj_3R_schema_character_set_or_path_4310_5_500()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1255()) {
    jj_scanpos = xsp;
    if (jj_3_1256()) {
    jj_scanpos = xsp;
    if (jj_3_1257()) {
    jj_scanpos = xsp;
    if (jj_3_1258()) return true;
    }
    }
    }
    return false;
  }

 inline bool jj_3_1255()
 {
    if (jj_done) return true;
    if (jj_3R_schema_character_set_specification_4327_5_502()) return true;
    return false;
  }

 inline bool jj_3_1254()
 {
    if (jj_done) return true;
    if (jj_3R_schema_element_4339_5_501()) return true;
    return false;
  }

 inline bool jj_3_1253()
 {
    if (jj_done) return true;
    if (jj_3R_schema_character_set_or_path_4310_5_500()) return true;
    return false;
  }

 inline bool jj_3_1252()
 {
    if (jj_done) return true;
    if (jj_3R_if_not_exists_7938_5_499()) return true;
    return false;
  }

 inline bool jj_3R_schema_definition_4298_5_739()
 {
    if (jj_done) return true;
    if (jj_scan_token(CREATE)) return true;
    if (jj_scan_token(SCHEMA)) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1252()) jj_scanpos = xsp;
    if (jj_3R_schema_name_clause_4319_5_992()) return true;
    return false;
  }

 inline bool jj_3_1251()
 {
    if (jj_done) return true;
    if (jj_scan_token(NULLS)) return true;
    if (jj_scan_token(LAST)) return true;
    return false;
  }

 inline bool jj_3R_null_ordering_4291_5_498()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1250()) {
    jj_scanpos = xsp;
    if (jj_3_1251()) return true;
    }
    return false;
  }

 inline bool jj_3_1250()
 {
    if (jj_done) return true;
    if (jj_scan_token(NULLS)) return true;
    if (jj_scan_token(FIRST)) return true;
    return false;
  }

 inline bool jj_3_1245()
 {
    if (jj_done) return true;
    if (jj_scan_token(570)) return true;
    if (jj_3R_sort_specification_4272_5_496()) return true;
    return false;
  }

 inline bool jj_3_1246()
 {
    if (jj_done) return true;
    if (jj_3R_ordering_specification_4284_5_497()) return true;
    return false;
  }

 inline bool jj_3_1249()
 {
    if (jj_done) return true;
    if (jj_scan_token(DESC)) return true;
    return false;
  }

 inline bool jj_3R_ordering_specification_4284_5_497()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1248()) {
    jj_scanpos = xsp;
    if (jj_3_1249()) return true;
    }
    return false;
  }

 inline bool jj_3_1248()
 {
    if (jj_done) return true;
    if (jj_scan_token(ASC)) return true;
    return false;
  }

 inline bool jj_3_1244()
 {
    if (jj_done) return true;
    if (jj_scan_token(ORDER)) return true;
    if (jj_scan_token(BY)) return true;
    if (jj_3R_sort_specification_list_4266_5_495()) return true;
    return false;
  }

 inline bool jj_3R_sort_key_4278_5_953()
 {
    if (jj_done) return true;
    if (jj_3R_value_expression_1855_5_178()) return true;
    return false;
  }

 inline bool jj_3R_sort_specification_4272_5_496()
 {
    if (jj_done) return true;
    if (jj_3R_sort_key_4278_5_953()) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1246()) jj_scanpos = xsp;
    xsp = jj_scanpos;
    if (jj_3_1247()) jj_scanpos = xsp;
    return false;
  }

 inline bool jj_3R_sort_specification_list_4266_5_495()
 {
    if (jj_done) return true;
    if (jj_3R_sort_specification_4272_5_496()) return true;
    Token * xsp;
    while (true) {
      xsp = jj_scanpos;
      if (jj_3_1245()) { jj_scanpos = xsp; break; }
    }
    return false;
  }

 inline bool jj_3_1243()
 {
    if (jj_done) return true;
    if (jj_scan_token(401)) return true;
    return false;
  }

 inline bool jj_3R_array_aggregate_function_4256_5_489()
 {
    if (jj_done) return true;
    if (jj_scan_token(ARRAY_AGG)) return true;
    if (jj_scan_token(lparen)) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1243()) jj_scanpos = xsp;
    if (jj_3R_value_expression_1855_5_178()) return true;
    xsp = jj_scanpos;
    if (jj_3_1244()) jj_scanpos = xsp;
    if (jj_scan_token(rparen)) return true;
    return false;
  }

 inline bool jj_3_1240()
 {
    if (jj_done) return true;
    if (jj_scan_token(570)) return true;
    if (jj_3R_value_expression_1855_5_178()) return true;
    return false;
  }

 inline bool jj_3_1242()
 {
    if (jj_done) return true;
    if (jj_scan_token(PERCENTILE_DISC)) return true;
    return false;
  }

 inline bool jj_3R_inverse_distribution_function_type_4249_5_951()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1241()) {
    jj_scanpos = xsp;
    if (jj_3_1242()) return true;
    }
    return false;
  }

 inline bool jj_3_1241()
 {
    if (jj_done) return true;
    if (jj_scan_token(PERCENTILE_CONT)) return true;
    return false;
  }

 inline bool jj_3R_inverse_distribution_function_argument_4243_5_952()
 {
    if (jj_done) return true;
    if (jj_3R_numeric_value_expression_1894_5_261()) return true;
    return false;
  }

 inline bool jj_3R_inverse_distribution_function_4235_5_494()
 {
    if (jj_done) return true;
    if (jj_3R_inverse_distribution_function_type_4249_5_951()) return true;
    if (jj_scan_token(lparen)) return true;
    if (jj_3R_inverse_distribution_function_argument_4243_5_952()) return true;
    if (jj_scan_token(rparen)) return true;
    if (jj_3R_within_group_specification_4223_5_1048()) return true;
    return false;
  }

 inline bool jj_3R_hypothetical_set_function_value_expression_list_4229_5_950()
 {
    if (jj_done) return true;
    if (jj_3R_value_expression_1855_5_178()) return true;
    Token * xsp;
    while (true) {
      xsp = jj_scanpos;
      if (jj_3_1240()) { jj_scanpos = xsp; break; }
    }
    return false;
  }

 inline bool jj_3R_within_group_specification_4223_5_1048()
 {
    if (jj_done) return true;
    if (jj_scan_token(WITHIN)) return true;
    if (jj_scan_token(GROUP)) return true;
    if (jj_scan_token(lparen)) return true;
    if (jj_scan_token(ORDER)) return true;
    if (jj_scan_token(BY)) return true;
    if (jj_3R_sort_specification_list_4266_5_495()) return true;
    if (jj_scan_token(rparen)) return true;
    return false;
  }

 inline bool jj_3R_hypothetical_set_function_4215_5_493()
 {
    if (jj_done) return true;
    if (jj_3R_rank_function_type_1541_5_213()) return true;
    if (jj_scan_token(lparen)) return true;
    if (jj_3R_hypothetical_set_function_value_expression_list_4229_5_950()) return true;
    if (jj_scan_token(rparen)) return true;
    if (jj_3R_within_group_specification_4223_5_1048()) return true;
    return false;
  }

 inline bool jj_3_1239()
 {
    if (jj_done) return true;
    if (jj_3R_inverse_distribution_function_4235_5_494()) return true;
    return false;
  }

 inline bool jj_3R_ordered_set_function_4208_5_488()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1238()) {
    jj_scanpos = xsp;
    if (jj_3_1239()) return true;
    }
    return false;
  }

 inline bool jj_3_1238()
 {
    if (jj_done) return true;
    if (jj_3R_hypothetical_set_function_4215_5_493()) return true;
    return false;
  }

 inline bool jj_3R_independent_variable_expression_4202_5_1045()
 {
    if (jj_done) return true;
    if (jj_3R_numeric_value_expression_1894_5_261()) return true;
    return false;
  }

 inline bool jj_3R_dependent_variable_expression_4196_5_948()
 {
    if (jj_done) return true;
    if (jj_3R_numeric_value_expression_1894_5_261()) return true;
    return false;
  }

 inline bool jj_3_1237()
 {
    if (jj_done) return true;
    if (jj_scan_token(REGR_SXY)) return true;
    return false;
  }

 inline bool jj_3_1236()
 {
    if (jj_done) return true;
    if (jj_scan_token(REGR_SYY)) return true;
    return false;
  }

 inline bool jj_3_1235()
 {
    if (jj_done) return true;
    if (jj_scan_token(REGR_SXX)) return true;
    return false;
  }

 inline bool jj_3_1234()
 {
    if (jj_done) return true;
    if (jj_scan_token(REGR_AVGY)) return true;
    return false;
  }

 inline bool jj_3_1233()
 {
    if (jj_done) return true;
    if (jj_scan_token(REGR_AVGX)) return true;
    return false;
  }

 inline bool jj_3_1232()
 {
    if (jj_done) return true;
    if (jj_scan_token(REGR_R2)) return true;
    return false;
  }

 inline bool jj_3_1231()
 {
    if (jj_done) return true;
    if (jj_scan_token(REGR_COUNT)) return true;
    return false;
  }

 inline bool jj_3_1230()
 {
    if (jj_done) return true;
    if (jj_scan_token(REGR_INTERCEPT)) return true;
    return false;
  }

 inline bool jj_3_1229()
 {
    if (jj_done) return true;
    if (jj_scan_token(REGR_SLOPE)) return true;
    return false;
  }

 inline bool jj_3_1228()
 {
    if (jj_done) return true;
    if (jj_scan_token(CORR)) return true;
    return false;
  }

 inline bool jj_3_1227()
 {
    if (jj_done) return true;
    if (jj_scan_token(COVAR_SAMP)) return true;
    return false;
  }

 inline bool jj_3R_binary_set_function_type_4179_5_947()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1226()) {
    jj_scanpos = xsp;
    if (jj_3_1227()) {
    jj_scanpos = xsp;
    if (jj_3_1228()) {
    jj_scanpos = xsp;
    if (jj_3_1229()) {
    jj_scanpos = xsp;
    if (jj_3_1230()) {
    jj_scanpos = xsp;
    if (jj_3_1231()) {
    jj_scanpos = xsp;
    if (jj_3_1232()) {
    jj_scanpos = xsp;
    if (jj_3_1233()) {
    jj_scanpos = xsp;
    if (jj_3_1234()) {
    jj_scanpos = xsp;
    if (jj_3_1235()) {
    jj_scanpos = xsp;
    if (jj_3_1236()) {
    jj_scanpos = xsp;
    if (jj_3_1237()) return true;
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    return false;
  }

 inline bool jj_3_1226()
 {
    if (jj_done) return true;
    if (jj_scan_token(COVAR_POP)) return true;
    return false;
  }

 inline bool jj_3R_binary_set_function_4172_5_487()
 {
    if (jj_done) return true;
    if (jj_3R_binary_set_function_type_4179_5_947()) return true;
    if (jj_scan_token(lparen)) return true;
    if (jj_3R_dependent_variable_expression_4196_5_948()) return true;
    if (jj_scan_token(570)) return true;
    if (jj_3R_independent_variable_expression_4202_5_1045()) return true;
    if (jj_scan_token(rparen)) return true;
    return false;
  }

 inline bool jj_3R_filter_clause_4166_5_491()
 {
    if (jj_done) return true;
    if (jj_scan_token(FILTER)) return true;
    if (jj_scan_token(lparen)) return true;
    if (jj_scan_token(WHERE)) return true;
    if (jj_3R_search_condition_3868_5_818()) return true;
    if (jj_scan_token(rparen)) return true;
    return false;
  }

 inline bool jj_3_1225()
 {
    if (jj_done) return true;
    if (jj_scan_token(ALL)) return true;
    return false;
  }

 inline bool jj_3R_set_quantifier_4159_5_396()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1224()) {
    jj_scanpos = xsp;
    if (jj_3_1225()) return true;
    }
    return false;
  }

 inline bool jj_3_1224()
 {
    if (jj_done) return true;
    if (jj_scan_token(DISTINCT)) return true;
    return false;
  }

 inline bool jj_3_1223()
 {
    if (jj_done) return true;
    if (jj_scan_token(INTERSECTION)) return true;
    return false;
  }

 inline bool jj_3_1222()
 {
    if (jj_done) return true;
    if (jj_scan_token(FUSION)) return true;
    return false;
  }

 inline bool jj_3_1207()
 {
    if (jj_done) return true;
    if (jj_3R_set_quantifier_4159_5_396()) return true;
    return false;
  }

 inline bool jj_3_1221()
 {
    if (jj_done) return true;
    if (jj_scan_token(COLLECT)) return true;
    return false;
  }

 inline bool jj_3_1220()
 {
    if (jj_done) return true;
    if (jj_scan_token(VAR_POP)) return true;
    return false;
  }

 inline bool jj_3_1219()
 {
    if (jj_done) return true;
    if (jj_scan_token(VAR_SAMP)) return true;
    return false;
  }

 inline bool jj_3_1218()
 {
    if (jj_done) return true;
    if (jj_scan_token(STDDEV_SAMP)) return true;
    return false;
  }

 inline bool jj_3_1217()
 {
    if (jj_done) return true;
    if (jj_scan_token(STDDEV_POP)) return true;
    return false;
  }

 inline bool jj_3_1216()
 {
    if (jj_done) return true;
    if (jj_scan_token(COUNT)) return true;
    return false;
  }

 inline bool jj_3_1215()
 {
    if (jj_done) return true;
    if (jj_scan_token(SOME)) return true;
    return false;
  }

 inline bool jj_3_1214()
 {
    if (jj_done) return true;
    if (jj_scan_token(ANY)) return true;
    return false;
  }

 inline bool jj_3_1213()
 {
    if (jj_done) return true;
    if (jj_scan_token(EVERY)) return true;
    return false;
  }

 inline bool jj_3_1192()
 {
    if (jj_done) return true;
    if (jj_3R_constraint_enforcement_4103_5_483()) return true;
    return false;
  }

 inline bool jj_3_1212()
 {
    if (jj_done) return true;
    if (jj_scan_token(SUM)) return true;
    return false;
  }

 inline bool jj_3_1189()
 {
    if (jj_done) return true;
    if (jj_3R_constraint_enforcement_4103_5_483()) return true;
    return false;
  }

 inline bool jj_3_1211()
 {
    if (jj_done) return true;
    if (jj_scan_token(MIN)) return true;
    return false;
  }

 inline bool jj_3_1210()
 {
    if (jj_done) return true;
    if (jj_scan_token(MAX)) return true;
    return false;
  }

 inline bool jj_3R_computational_operation_4139_5_1028()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1209()) {
    jj_scanpos = xsp;
    if (jj_3_1210()) {
    jj_scanpos = xsp;
    if (jj_3_1211()) {
    jj_scanpos = xsp;
    if (jj_3_1212()) {
    jj_scanpos = xsp;
    if (jj_3_1213()) {
    jj_scanpos = xsp;
    if (jj_3_1214()) {
    jj_scanpos = xsp;
    if (jj_3_1215()) {
    jj_scanpos = xsp;
    if (jj_3_1216()) {
    jj_scanpos = xsp;
    if (jj_3_1217()) {
    jj_scanpos = xsp;
    if (jj_3_1218()) {
    jj_scanpos = xsp;
    if (jj_3_1219()) {
    jj_scanpos = xsp;
    if (jj_3_1220()) {
    jj_scanpos = xsp;
    if (jj_3_1221()) {
    jj_scanpos = xsp;
    if (jj_3_1222()) {
    jj_scanpos = xsp;
    if (jj_3_1223()) return true;
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    return false;
  }

 inline bool jj_3_1209()
 {
    if (jj_done) return true;
    if (jj_scan_token(AVG)) return true;
    return false;
  }

 inline bool jj_3R_set_function_type_4133_5_946()
 {
    if (jj_done) return true;
    if (jj_3R_computational_operation_4139_5_1028()) return true;
    return false;
  }

 inline bool jj_3_1208()
 {
    if (jj_done) return true;
    if (jj_3R_extra_args_to_agg_8069_4_492()) return true;
    return false;
  }

 inline bool jj_3R_general_set_function_4125_5_486()
 {
    if (jj_done) return true;
    if (jj_3R_set_function_type_4133_5_946()) return true;
    if (jj_scan_token(lparen)) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1207()) jj_scanpos = xsp;
    if (jj_3R_value_expression_1855_5_178()) return true;
    xsp = jj_scanpos;
    if (jj_3_1208()) jj_scanpos = xsp;
    if (jj_scan_token(rparen)) return true;
    return false;
  }

 inline bool jj_3_1206()
 {
    if (jj_done) return true;
    if (jj_3R_filter_clause_4166_5_491()) return true;
    return false;
  }

 inline bool jj_3_1187()
 {
    if (jj_done) return true;
    if (jj_scan_token(NOT)) return true;
    return false;
  }

 inline bool jj_3_1205()
 {
    if (jj_done) return true;
    if (jj_3R_presto_aggregations_8032_5_490()) return true;
    return false;
  }

 inline bool jj_3_1204()
 {
    if (jj_done) return true;
    if (jj_3R_array_aggregate_function_4256_5_489()) return true;
    return false;
  }

 inline bool jj_3_1188()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1187()) jj_scanpos = xsp;
    if (jj_scan_token(DEFERRABLE)) return true;
    return false;
  }

 inline bool jj_3_1191()
 {
    if (jj_done) return true;
    if (jj_3R_constraint_check_time_4096_5_484()) return true;
    return false;
  }

 inline bool jj_3_1203()
 {
    if (jj_done) return true;
    if (jj_3R_ordered_set_function_4208_5_488()) return true;
    return false;
  }

 inline bool jj_3_1202()
 {
    if (jj_done) return true;
    if (jj_3R_binary_set_function_4172_5_487()) return true;
    return false;
  }

 inline bool jj_3_1201()
 {
    if (jj_done) return true;
    if (jj_3R_general_set_function_4125_5_486()) return true;
    return false;
  }

 inline bool jj_3_1200()
 {
    if (jj_done) return true;
    if (jj_3R_count_7996_5_485()) return true;
    return false;
  }

 inline bool jj_3_1199()
 {
    if (jj_done) return true;
    if (jj_scan_token(COUNT)) return true;
    if (jj_scan_token(lparen)) return true;
    if (jj_scan_token(STAR)) return true;
    if (jj_scan_token(rparen)) return true;
    return false;
  }

 inline bool jj_3R_aggregate_function_4109_3_211()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1199()) {
    jj_scanpos = xsp;
    if (jj_3_1200()) {
    jj_scanpos = xsp;
    if (jj_3_1201()) {
    jj_scanpos = xsp;
    if (jj_3_1202()) {
    jj_scanpos = xsp;
    if (jj_3_1203()) {
    jj_scanpos = xsp;
    if (jj_3_1204()) {
    jj_scanpos = xsp;
    if (jj_3_1205()) return true;
    }
    }
    }
    }
    }
    }
    xsp = jj_scanpos;
    if (jj_3_1206()) jj_scanpos = xsp;
    return false;
  }

 inline bool jj_3_1198()
 {
    if (jj_done) return true;
    if (jj_scan_token(NOT)) return true;
    return false;
  }

 inline bool jj_3R_constraint_enforcement_4103_5_483()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1198()) jj_scanpos = xsp;
    if (jj_scan_token(ENFORCED)) return true;
    return false;
  }

 inline bool jj_3_1197()
 {
    if (jj_done) return true;
    if (jj_scan_token(INITIALLY)) return true;
    if (jj_scan_token(IMMEDIATE)) return true;
    return false;
  }

 inline bool jj_3R_constraint_check_time_4096_5_484()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1196()) {
    jj_scanpos = xsp;
    if (jj_3_1197()) return true;
    }
    return false;
  }

 inline bool jj_3_1196()
 {
    if (jj_done) return true;
    if (jj_scan_token(INITIALLY)) return true;
    if (jj_scan_token(DEFERRED)) return true;
    return false;
  }

 inline bool jj_3_1185()
 {
    if (jj_done) return true;
    if (jj_scan_token(570)) return true;
    if (jj_3R_data_type_1086_3_251()) return true;
    return false;
  }

 inline bool jj_3_1190()
 {
    if (jj_done) return true;
    if (jj_scan_token(NOT)) return true;
    return false;
  }

 inline bool jj_3_1195()
 {
    if (jj_done) return true;
    if (jj_3R_constraint_enforcement_4103_5_483()) return true;
    return false;
  }

 inline bool jj_3_1194()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1190()) jj_scanpos = xsp;
    if (jj_scan_token(DEFERRABLE)) return true;
    xsp = jj_scanpos;
    if (jj_3_1191()) jj_scanpos = xsp;
    xsp = jj_scanpos;
    if (jj_3_1192()) jj_scanpos = xsp;
    return false;
  }

 inline bool jj_3R_constraint_characteristics_4088_5_556()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1193()) {
    jj_scanpos = xsp;
    if (jj_3_1194()) {
    jj_scanpos = xsp;
    if (jj_3_1195()) return true;
    }
    }
    return false;
  }

 inline bool jj_3_1193()
 {
    if (jj_done) return true;
    if (jj_3R_constraint_check_time_4096_5_484()) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1188()) jj_scanpos = xsp;
    xsp = jj_scanpos;
    if (jj_3_1189()) jj_scanpos = xsp;
    return false;
  }

 inline bool jj_3_1182()
 {
    if (jj_done) return true;
    if (jj_3R_data_type_list_4070_6_482()) return true;
    return false;
  }

 inline bool jj_3R_constraint_name_definition_4082_5_555()
 {
    if (jj_done) return true;
    if (jj_scan_token(CONSTRAINT)) return true;
    if (jj_3R_schema_qualified_name_970_5_252()) return true;
    return false;
  }

 inline bool jj_3_1176()
 {
    if (jj_done) return true;
    if (jj_scan_token(CONSTRUCTOR)) return true;
    return false;
  }

 inline bool jj_3_1186()
 {
    if (jj_done) return true;
    if (jj_3R_data_type_1086_3_251()) return true;
    Token * xsp;
    while (true) {
      xsp = jj_scanpos;
      if (jj_3_1185()) { jj_scanpos = xsp; break; }
    }
    return false;
  }

 inline bool jj_3R_collate_clause_4076_5_153()
 {
    if (jj_done) return true;
    if (jj_scan_token(COLLATE)) return true;
    if (jj_3R_schema_qualified_name_970_5_252()) return true;
    return false;
  }

 inline bool jj_3_1171()
 {
    if (jj_done) return true;
    if (jj_scan_token(FOR)) return true;
    if (jj_3R_schema_resolved_user_defined_type_name_1026_5_479()) return true;
    return false;
  }

 inline bool jj_3R_data_type_list_4070_6_482()
 {
    if (jj_done) return true;
    if (jj_scan_token(lparen)) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1186()) jj_scanpos = xsp;
    if (jj_scan_token(rparen)) return true;
    return false;
  }

 inline bool jj_3_1175()
 {
    if (jj_done) return true;
    if (jj_scan_token(STATIC)) return true;
    return false;
  }

 inline bool jj_3_1184()
 {
    if (jj_done) return true;
    if (jj_3R_identifier_928_3_141()) return true;
    return false;
  }

 inline bool jj_3R_member_name_alternatives_4063_5_945()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1183()) {
    jj_scanpos = xsp;
    if (jj_3_1184()) return true;
    }
    return false;
  }

 inline bool jj_3_1183()
 {
    if (jj_done) return true;
    if (jj_3R_schema_qualified_name_970_5_252()) return true;
    return false;
  }

 inline bool jj_3R_member_name_4057_5_481()
 {
    if (jj_done) return true;
    if (jj_3R_member_name_alternatives_4063_5_945()) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1182()) jj_scanpos = xsp;
    return false;
  }

 inline bool jj_3_1174()
 {
    if (jj_done) return true;
    if (jj_scan_token(INSTANCE)) return true;
    return false;
  }

 inline bool jj_3_1177()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1174()) {
    jj_scanpos = xsp;
    if (jj_3_1175()) {
    jj_scanpos = xsp;
    if (jj_3_1176()) return true;
    }
    }
    return false;
  }

 inline bool jj_3_1181()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1177()) jj_scanpos = xsp;
    if (jj_scan_token(METHOD)) return true;
    return false;
  }

 inline bool jj_3_1180()
 {
    if (jj_done) return true;
    if (jj_scan_token(PROCEDURE)) return true;
    return false;
  }

 inline bool jj_3_1179()
 {
    if (jj_done) return true;
    if (jj_scan_token(FUNCTION)) return true;
    return false;
  }

 inline bool jj_3R_routine_type_4048_5_480()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1178()) {
    jj_scanpos = xsp;
    if (jj_3_1179()) {
    jj_scanpos = xsp;
    if (jj_3_1180()) {
    jj_scanpos = xsp;
    if (jj_3_1181()) return true;
    }
    }
    }
    return false;
  }

 inline bool jj_3_1178()
 {
    if (jj_done) return true;
    if (jj_scan_token(ROUTINE)) return true;
    return false;
  }

 inline bool jj_3_1173()
 {
    if (jj_done) return true;
    if (jj_3R_routine_type_4048_5_480()) return true;
    if (jj_3R_member_name_4057_5_481()) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1171()) jj_scanpos = xsp;
    return false;
  }

 inline bool jj_3R_specific_routine_designator_4041_5_708()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1172()) {
    jj_scanpos = xsp;
    if (jj_3_1173()) return true;
    }
    return false;
  }

 inline bool jj_3_1172()
 {
    if (jj_done) return true;
    if (jj_scan_token(SPECIFIC)) return true;
    if (jj_3R_routine_type_4048_5_480()) return true;
    if (jj_3R_schema_qualified_name_970_5_252()) return true;
    return false;
  }

 inline bool jj_3R_user_defined_character_set_name_4035_5_478()
 {
    if (jj_done) return true;
    if (jj_3R_character_set_name_1020_5_707()) return true;
    return false;
  }

 inline bool jj_3R_implementation_defined_character_set_name_4029_5_477()
 {
    if (jj_done) return true;
    if (jj_3R_character_set_name_1020_5_707()) return true;
    return false;
  }

 inline bool jj_3R_standard_character_set_name_4023_5_476()
 {
    if (jj_done) return true;
    if (jj_3R_character_set_name_1020_5_707()) return true;
    return false;
  }

 inline bool jj_3_1170()
 {
    if (jj_done) return true;
    if (jj_3R_user_defined_character_set_name_4035_5_478()) return true;
    return false;
  }

 inline bool jj_3_1169()
 {
    if (jj_done) return true;
    if (jj_3R_implementation_defined_character_set_name_4029_5_477()) return true;
    return false;
  }

 inline bool jj_3_1168()
 {
    if (jj_done) return true;
    if (jj_3R_standard_character_set_name_4023_5_476()) return true;
    return false;
  }

 inline bool jj_3R_character_set_specification_4015_5_133()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1168()) {
    jj_scanpos = xsp;
    if (jj_3_1169()) {
    jj_scanpos = xsp;
    if (jj_3_1170()) return true;
    }
    }
    return false;
  }

 inline bool jj_3_1167()
 {
    if (jj_done) return true;
    if (jj_3R_target_specification_1434_3_475()) return true;
    return false;
  }

 inline bool jj_3_1166()
 {
    if (jj_done) return true;
    if (jj_3R_contextually_typed_value_specification_1475_5_194()) return true;
    return false;
  }

 inline bool jj_3R_named_argument_SQL_argument_4007_5_944()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1165()) {
    jj_scanpos = xsp;
    if (jj_3_1166()) {
    jj_scanpos = xsp;
    if (jj_3_1167()) return true;
    }
    }
    return false;
  }

 inline bool jj_3_1165()
 {
    if (jj_done) return true;
    if (jj_3R_value_expression_1855_5_178()) return true;
    return false;
  }

 inline bool jj_3_1159()
 {
    if (jj_done) return true;
    if (jj_3R_generalized_expression_3994_5_472()) return true;
    return false;
  }

 inline bool jj_3_1157()
 {
    if (jj_done) return true;
    if (jj_scan_token(570)) return true;
    if (jj_3R_SQL_argument_3984_5_471()) return true;
    return false;
  }

 inline bool jj_3R_named_argument_specification_4000_5_474()
 {
    if (jj_done) return true;
    if (jj_3R_identifier_928_3_141()) return true;
    if (jj_scan_token(584)) return true;
    if (jj_3R_named_argument_SQL_argument_4007_5_944()) return true;
    return false;
  }

 inline bool jj_3R_generalized_expression_3994_5_472()
 {
    if (jj_done) return true;
    if (jj_scan_token(AS)) return true;
    if (jj_3R_path_resolved_user_defined_type_name_1281_5_151()) return true;
    return false;
  }

 inline bool jj_3_1160()
 {
    if (jj_done) return true;
    if (jj_3R_lambda_params_7931_5_473()) return true;
    if (jj_scan_token(573)) return true;
    return false;
  }

 inline bool jj_3_1164()
 {
    if (jj_done) return true;
    if (jj_3R_target_specification_1434_3_475()) return true;
    return false;
  }

 inline bool jj_3_1163()
 {
    if (jj_done) return true;
    if (jj_3R_contextually_typed_value_specification_1475_5_194()) return true;
    return false;
  }

 inline bool jj_3_1162()
 {
    if (jj_done) return true;
    if (jj_3R_named_argument_specification_4000_5_474()) return true;
    return false;
  }

 inline bool jj_3_1158()
 {
    if (jj_done) return true;
    if (jj_3R_SQL_argument_3984_5_471()) return true;
    Token * xsp;
    while (true) {
      xsp = jj_scanpos;
      if (jj_3_1157()) { jj_scanpos = xsp; break; }
    }
    return false;
  }

 inline bool jj_3_1161()
 {
    if (jj_done) return true;
    if (jj_3R_value_expression_1855_5_178()) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1159()) jj_scanpos = xsp;
    return false;
  }

 inline bool jj_3R_SQL_argument_3984_5_943()
 {
    if (jj_done) return true;
    if (jj_3R_lambda_7919_5_1027()) return true;
    return false;
  }

 inline bool jj_3R_SQL_argument_3984_5_471()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3R_SQL_argument_3984_5_943()) {
    jj_scanpos = xsp;
    if (jj_3_1161()) {
    jj_scanpos = xsp;
    if (jj_3_1162()) {
    jj_scanpos = xsp;
    if (jj_3_1163()) {
    jj_scanpos = xsp;
    if (jj_3_1164()) return true;
    }
    }
    }
    }
    return false;
  }

 inline bool jj_3R_SQL_argument_list_3978_6_256()
 {
    if (jj_done) return true;
    if (jj_scan_token(lparen)) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1158()) jj_scanpos = xsp;
    if (jj_scan_token(rparen)) return true;
    return false;
  }

 inline bool jj_3_1155()
 {
    if (jj_done) return true;
    if (jj_scan_token(570)) return true;
    if (jj_3R_schema_name_956_5_140()) return true;
    return false;
  }

 inline bool jj_3_1156()
 {
    if (jj_done) return true;
    if (jj_3R_schema_name_956_5_140()) return true;
    if (jj_scan_token(569)) return true;
    return false;
  }

 inline bool jj_3R_routine_name_3972_5_906()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1156()) jj_scanpos = xsp;
    if (jj_3R_identifier_928_3_141()) return true;
    return false;
  }

 inline bool jj_3R_routine_invocation_3966_5_257()
 {
    if (jj_done) return true;
    if (jj_3R_routine_name_3972_5_906()) return true;
    if (jj_3R_SQL_argument_list_3978_6_256()) return true;
    return false;
  }

 inline bool jj_3R_schema_name_list_3960_5_1029()
 {
    if (jj_done) return true;
    if (jj_3R_schema_name_956_5_140()) return true;
    Token * xsp;
    while (true) {
      xsp = jj_scanpos;
      if (jj_3_1155()) { jj_scanpos = xsp; break; }
    }
    return false;
  }

 inline bool jj_3R_path_specification_3954_5_954()
 {
    if (jj_done) return true;
    if (jj_scan_token(PATH)) return true;
    if (jj_3R_schema_name_list_3960_5_1029()) return true;
    return false;
  }

 inline bool jj_3_1151()
 {
    if (jj_done) return true;
    if (jj_scan_token(MUMPS)) return true;
    return false;
  }

 inline bool jj_3_1154()
 {
    if (jj_done) return true;
    if (jj_scan_token(SQL)) return true;
    return false;
  }

 inline bool jj_3_1153()
 {
    if (jj_done) return true;
    if (jj_scan_token(PLI)) return true;
    return false;
  }

 inline bool jj_3_1152()
 {
    if (jj_done) return true;
    if (jj_scan_token(PASCAL)) return true;
    return false;
  }

 inline bool jj_3_1150()
 {
    if (jj_done) return true;
    if (jj_scan_token(M)) return true;
    return false;
  }

 inline bool jj_3_1149()
 {
    if (jj_done) return true;
    if (jj_scan_token(FORTRAN)) return true;
    return false;
  }

 inline bool jj_3_1148()
 {
    if (jj_done) return true;
    if (jj_scan_token(COBOL)) return true;
    return false;
  }

 inline bool jj_3_1147()
 {
    if (jj_done) return true;
    if (jj_scan_token(C)) return true;
    return false;
  }

 inline bool jj_3R_language_name_3941_5_972()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1146()) {
    jj_scanpos = xsp;
    if (jj_3_1147()) {
    jj_scanpos = xsp;
    if (jj_3_1148()) {
    jj_scanpos = xsp;
    if (jj_3_1149()) {
    jj_scanpos = xsp;
    if (jj_3_1150()) {
    jj_scanpos = xsp;
    if (jj_3_1151()) {
    jj_scanpos = xsp;
    if (jj_3_1152()) {
    jj_scanpos = xsp;
    if (jj_3_1153()) {
    jj_scanpos = xsp;
    if (jj_3_1154()) return true;
    }
    }
    }
    }
    }
    }
    }
    }
    return false;
  }

 inline bool jj_3_1146()
 {
    if (jj_done) return true;
    if (jj_scan_token(ADA)) return true;
    return false;
  }

 inline bool jj_3R_language_clause_3935_5_635()
 {
    if (jj_done) return true;
    if (jj_scan_token(LANGUAGE)) return true;
    if (jj_3R_language_name_3941_5_972()) return true;
    return false;
  }

 inline bool jj_3_1145()
 {
    if (jj_done) return true;
    if (jj_scan_token(MINUTE)) return true;
    return false;
  }

 inline bool jj_3_1144()
 {
    if (jj_done) return true;
    if (jj_scan_token(HOUR)) return true;
    return false;
  }

 inline bool jj_3_1143()
 {
    if (jj_done) return true;
    if (jj_scan_token(DAY)) return true;
    return false;
  }

 inline bool jj_3_1142()
 {
    if (jj_done) return true;
    if (jj_scan_token(MONTH)) return true;
    return false;
  }

 inline bool jj_3R_non_second_primary_datetime_field_3913_5_470()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1141()) {
    jj_scanpos = xsp;
    if (jj_3_1142()) {
    jj_scanpos = xsp;
    if (jj_3_1143()) {
    jj_scanpos = xsp;
    if (jj_3_1144()) {
    jj_scanpos = xsp;
    if (jj_3_1145()) return true;
    }
    }
    }
    }
    return false;
  }

 inline bool jj_3_1141()
 {
    if (jj_done) return true;
    if (jj_scan_token(YEAR)) return true;
    return false;
  }

 inline bool jj_3_1136()
 {
    if (jj_done) return true;
    if (jj_scan_token(lparen)) return true;
    if (jj_scan_token(633)) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1135()) jj_scanpos = xsp;
    if (jj_scan_token(rparen)) return true;
    return false;
  }

 inline bool jj_3_1140()
 {
    if (jj_done) return true;
    if (jj_scan_token(SECOND)) return true;
    return false;
  }

 inline bool jj_3R_primary_datetime_field_3906_5_296()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1139()) {
    jj_scanpos = xsp;
    if (jj_3_1140()) return true;
    }
    return false;
  }

 inline bool jj_3_1139()
 {
    if (jj_done) return true;
    if (jj_3R_non_second_primary_datetime_field_3913_5_470()) return true;
    return false;
  }

 inline bool jj_3_1135()
 {
    if (jj_done) return true;
    if (jj_scan_token(570)) return true;
    if (jj_scan_token(633)) return true;
    return false;
  }

 inline bool jj_3_1132()
 {
    if (jj_done) return true;
    if (jj_scan_token(lparen)) return true;
    if (jj_scan_token(633)) return true;
    if (jj_scan_token(rparen)) return true;
    return false;
  }

 inline bool jj_3_1138()
 {
    if (jj_done) return true;
    if (jj_scan_token(SECOND)) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1136()) jj_scanpos = xsp;
    return false;
  }

 inline bool jj_3R_single_datetime_field_3898_5_469()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1137()) {
    jj_scanpos = xsp;
    if (jj_3_1138()) return true;
    }
    return false;
  }

 inline bool jj_3_1137()
 {
    if (jj_done) return true;
    if (jj_3R_start_field_3881_5_467()) return true;
    return false;
  }

 inline bool jj_3_1134()
 {
    if (jj_done) return true;
    if (jj_scan_token(SECOND)) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1132()) jj_scanpos = xsp;
    return false;
  }

 inline bool jj_3R_end_field_3888_5_468()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1133()) {
    jj_scanpos = xsp;
    if (jj_3_1134()) return true;
    }
    return false;
  }

 inline bool jj_3_1133()
 {
    if (jj_done) return true;
    if (jj_3R_non_second_primary_datetime_field_3913_5_470()) return true;
    return false;
  }

 inline bool jj_3_1131()
 {
    if (jj_done) return true;
    if (jj_scan_token(lparen)) return true;
    if (jj_scan_token(633)) return true;
    if (jj_scan_token(rparen)) return true;
    return false;
  }

 inline bool jj_3R_start_field_3881_5_467()
 {
    if (jj_done) return true;
    if (jj_3R_non_second_primary_datetime_field_3913_5_470()) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1131()) jj_scanpos = xsp;
    return false;
  }

 inline bool jj_3_1130()
 {
    if (jj_done) return true;
    if (jj_3R_single_datetime_field_3898_5_469()) return true;
    return false;
  }

 inline bool jj_3R_interval_qualifier_3874_5_331()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1129()) {
    jj_scanpos = xsp;
    if (jj_3_1130()) return true;
    }
    return false;
  }

 inline bool jj_3_1129()
 {
    if (jj_done) return true;
    if (jj_3R_start_field_3881_5_467()) return true;
    if (jj_scan_token(TO)) return true;
    if (jj_3R_end_field_3888_5_468()) return true;
    return false;
  }

 inline bool jj_3R_search_condition_3868_5_818()
 {
    if (jj_done) return true;
    if (jj_3R_boolean_value_expression_2494_5_258()) return true;
    return false;
  }

 inline bool jj_3R_exclusive_user_defined_type_specification_3862_5_465()
 {
    if (jj_done) return true;
    if (jj_scan_token(ONLY)) return true;
    if (jj_3R_path_resolved_user_defined_type_name_1281_5_151()) return true;
    return false;
  }

 inline bool jj_3R_inclusive_user_defined_type_specification_3856_5_466()
 {
    if (jj_done) return true;
    if (jj_3R_path_resolved_user_defined_type_name_1281_5_151()) return true;
    return false;
  }

 inline bool jj_3_1128()
 {
    if (jj_done) return true;
    if (jj_3R_inclusive_user_defined_type_specification_3856_5_466()) return true;
    return false;
  }

 inline bool jj_3R_user_defined_type_specification_3849_5_464()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1127()) {
    jj_scanpos = xsp;
    if (jj_3_1128()) return true;
    }
    return false;
  }

 inline bool jj_3_1127()
 {
    if (jj_done) return true;
    if (jj_3R_exclusive_user_defined_type_specification_3862_5_465()) return true;
    return false;
  }

 inline bool jj_3_1126()
 {
    if (jj_done) return true;
    if (jj_scan_token(570)) return true;
    if (jj_3R_user_defined_type_specification_3849_5_464()) return true;
    return false;
  }

 inline bool jj_3_1125()
 {
    if (jj_done) return true;
    if (jj_scan_token(NOT)) return true;
    return false;
  }

 inline bool jj_3R_type_list_3842_5_1050()
 {
    if (jj_done) return true;
    if (jj_3R_user_defined_type_specification_3849_5_464()) return true;
    Token * xsp;
    while (true) {
      xsp = jj_scanpos;
      if (jj_3_1126()) { jj_scanpos = xsp; break; }
    }
    return false;
  }

 inline bool jj_3_1123()
 {
    if (jj_done) return true;
    if (jj_scan_token(OF)) return true;
    return false;
  }

 inline bool jj_3R_type_predicate_part_2_3836_5_248()
 {
    if (jj_done) return true;
    if (jj_scan_token(IS)) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1125()) jj_scanpos = xsp;
    if (jj_scan_token(OF)) return true;
    if (jj_scan_token(lparen)) return true;
    if (jj_3R_type_list_3842_5_1050()) return true;
    if (jj_scan_token(rparen)) return true;
    return false;
  }

 inline bool jj_3_1124()
 {
    if (jj_done) return true;
    if (jj_scan_token(NOT)) return true;
    return false;
  }

 inline bool jj_3R_type_predicate_3830_5_458()
 {
    if (jj_done) return true;
    if (jj_3R_type_predicate_part_2_3836_5_248()) return true;
    return false;
  }

 inline bool jj_3R_set_predicate_part_2_3824_5_247()
 {
    if (jj_done) return true;
    if (jj_scan_token(IS)) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1124()) jj_scanpos = xsp;
    if (jj_scan_token(A)) return true;
    if (jj_scan_token(SET)) return true;
    return false;
  }

 inline bool jj_3_1121()
 {
    if (jj_done) return true;
    if (jj_scan_token(OF)) return true;
    return false;
  }

 inline bool jj_3R_set_predicate_3818_5_457()
 {
    if (jj_done) return true;
    if (jj_3R_set_predicate_part_2_3824_5_247()) return true;
    return false;
  }

 inline bool jj_3_1122()
 {
    if (jj_done) return true;
    if (jj_scan_token(NOT)) return true;
    return false;
  }

 inline bool jj_3R_submultiset_predicate_part_2_3812_5_246()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1122()) jj_scanpos = xsp;
    if (jj_scan_token(SUBMULTISET)) return true;
    xsp = jj_scanpos;
    if (jj_3_1123()) jj_scanpos = xsp;
    if (jj_3R_multiset_value_expression_2612_5_269()) return true;
    return false;
  }

 inline bool jj_3R_submultiset_predicate_3806_5_456()
 {
    if (jj_done) return true;
    if (jj_3R_submultiset_predicate_part_2_3812_5_246()) return true;
    return false;
  }

 inline bool jj_3_1120()
 {
    if (jj_done) return true;
    if (jj_scan_token(NOT)) return true;
    return false;
  }

 inline bool jj_3R_member_predicate_part_2_3800_5_245()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1120()) jj_scanpos = xsp;
    if (jj_scan_token(MEMBER)) return true;
    xsp = jj_scanpos;
    if (jj_3_1121()) jj_scanpos = xsp;
    if (jj_3R_multiset_value_expression_2612_5_269()) return true;
    return false;
  }

 inline bool jj_3R_member_predicate_3794_5_455()
 {
    if (jj_done) return true;
    if (jj_3R_member_predicate_part_2_3800_5_245()) return true;
    return false;
  }

 inline bool jj_3R_row_value_predicand_4_3788_5_1049()
 {
    if (jj_done) return true;
    if (jj_3R_row_value_predicand_2758_5_229()) return true;
    return false;
  }

 inline bool jj_3_1119()
 {
    if (jj_done) return true;
    if (jj_scan_token(NOT)) return true;
    return false;
  }

 inline bool jj_3_1117()
 {
    if (jj_done) return true;
    if (jj_scan_token(FULL)) return true;
    return false;
  }

 inline bool jj_3R_distinct_predicate_part_2_3776_5_244()
 {
    if (jj_done) return true;
    if (jj_scan_token(IS)) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1119()) jj_scanpos = xsp;
    if (jj_scan_token(DISTINCT)) return true;
    if (jj_scan_token(FROM)) return true;
    if (jj_3R_row_value_predicand_4_3788_5_1049()) return true;
    return false;
  }

 inline bool jj_3R_distinct_predicate_3770_5_454()
 {
    if (jj_done) return true;
    if (jj_3R_distinct_predicate_part_2_3776_5_244()) return true;
    return false;
  }

 inline bool jj_3_1116()
 {
    if (jj_done) return true;
    if (jj_scan_token(PARTIAL)) return true;
    return false;
  }

 inline bool jj_3R_row_value_predicand_2_3764_5_905()
 {
    if (jj_done) return true;
    if (jj_3R_row_value_predicand_2758_5_229()) return true;
    return false;
  }

 inline bool jj_3R_row_value_predicand_1_3758_5_902()
 {
    if (jj_done) return true;
    if (jj_3R_row_value_predicand_2758_5_229()) return true;
    return false;
  }

 inline bool jj_3_1118()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1115()) {
    jj_scanpos = xsp;
    if (jj_3_1116()) {
    jj_scanpos = xsp;
    if (jj_3_1117()) return true;
    }
    }
    return false;
  }

 inline bool jj_3_1115()
 {
    if (jj_done) return true;
    if (jj_scan_token(SIMPLE)) return true;
    return false;
  }

 inline bool jj_3R_overlaps_predicate_part_2_3752_5_243()
 {
    if (jj_done) return true;
    if (jj_scan_token(OVERLAPS)) return true;
    if (jj_3R_row_value_predicand_2_3764_5_905()) return true;
    return false;
  }

 inline bool jj_3R_overlaps_predicate_part_1_3746_5_230()
 {
    if (jj_done) return true;
    if (jj_3R_row_value_predicand_1_3758_5_902()) return true;
    return false;
  }

 inline bool jj_3_1114()
 {
    if (jj_done) return true;
    if (jj_scan_token(UNIQUE)) return true;
    return false;
  }

 inline bool jj_3R_overlaps_predicate_3740_5_453()
 {
    if (jj_done) return true;
    if (jj_3R_overlaps_predicate_part_2_3752_5_243()) return true;
    return false;
  }

 inline bool jj_3_1113()
 {
    if (jj_done) return true;
    if (jj_3R_normal_form_2306_5_318()) return true;
    return false;
  }

 inline bool jj_3R_match_predicate_part_2_3734_5_242()
 {
    if (jj_done) return true;
    if (jj_scan_token(MATCH)) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1114()) jj_scanpos = xsp;
    xsp = jj_scanpos;
    if (jj_3_1118()) jj_scanpos = xsp;
    if (jj_3R_subquery_3527_5_181()) return true;
    return false;
  }

 inline bool jj_3_1112()
 {
    if (jj_done) return true;
    if (jj_scan_token(NOT)) return true;
    return false;
  }

 inline bool jj_3R_match_predicate_3728_5_452()
 {
    if (jj_done) return true;
    if (jj_3R_match_predicate_part_2_3734_5_242()) return true;
    return false;
  }

 inline bool jj_3_1107()
 {
    if (jj_done) return true;
    if (jj_scan_token(FLAG)) return true;
    if (jj_3R_character_value_expression_2128_5_274()) return true;
    return false;
  }

 inline bool jj_3_1111()
 {
    if (jj_done) return true;
    if (jj_scan_token(ANY)) return true;
    return false;
  }

 inline bool jj_3R_normalized_predicate_part_2_3722_5_241()
 {
    if (jj_done) return true;
    if (jj_scan_token(IS)) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1112()) jj_scanpos = xsp;
    xsp = jj_scanpos;
    if (jj_3_1113()) jj_scanpos = xsp;
    if (jj_scan_token(NORMALIZED)) return true;
    return false;
  }

 inline bool jj_3_1110()
 {
    if (jj_done) return true;
    if (jj_scan_token(SOME)) return true;
    return false;
  }

 inline bool jj_3_1105()
 {
    if (jj_done) return true;
    if (jj_scan_token(ESCAPE)) return true;
    if (jj_3R_character_value_expression_2128_5_274()) return true;
    return false;
  }

 inline bool jj_3R_normalized_predicate_3716_5_451()
 {
    if (jj_done) return true;
    if (jj_3R_normalized_predicate_part_2_3722_5_241()) return true;
    return false;
  }

 inline bool jj_3R_unique_predicate_3710_5_460()
 {
    if (jj_done) return true;
    if (jj_scan_token(UNIQUE)) return true;
    if (jj_3R_subquery_3527_5_181()) return true;
    return false;
  }

 inline bool jj_3_1109()
 {
    if (jj_done) return true;
    if (jj_scan_token(ALL)) return true;
    return false;
  }

 inline bool jj_3R_exists_predicate_3704_5_459()
 {
    if (jj_done) return true;
    if (jj_scan_token(EXISTS)) return true;
    if (jj_3R_subquery_3527_5_181()) return true;
    return false;
  }

 inline bool jj_3R_quantified_comparison_predicate_part_2_3698_5_240()
 {
    if (jj_done) return true;
    if (jj_3R_comp_op_3575_5_903()) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1109()) {
    jj_scanpos = xsp;
    if (jj_3_1110()) {
    jj_scanpos = xsp;
    if (jj_3_1111()) return true;
    }
    }
    if (jj_3R_subquery_3527_5_181()) return true;
    return false;
  }

 inline bool jj_3_1103()
 {
    if (jj_done) return true;
    if (jj_scan_token(ESCAPE)) return true;
    if (jj_3R_binary_value_expression_2154_5_298()) return true;
    return false;
  }

 inline bool jj_3_1108()
 {
    if (jj_done) return true;
    if (jj_scan_token(NOT)) return true;
    return false;
  }

 inline bool jj_3R_quantified_comparison_predicate_3692_5_450()
 {
    if (jj_done) return true;
    if (jj_3R_quantified_comparison_predicate_part_2_3698_5_240()) return true;
    return false;
  }

 inline bool jj_3_1101()
 {
    if (jj_done) return true;
    if (jj_scan_token(ESCAPE)) return true;
    if (jj_3R_character_value_expression_2128_5_274()) return true;
    return false;
  }

 inline bool jj_3R_null_predicate_part_2_3686_5_239()
 {
    if (jj_done) return true;
    if (jj_scan_token(IS)) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1108()) jj_scanpos = xsp;
    if (jj_scan_token(NULL_)) return true;
    return false;
  }

 inline bool jj_3R_null_predicate_3680_5_449()
 {
    if (jj_done) return true;
    if (jj_3R_null_predicate_part_2_3686_5_239()) return true;
    return false;
  }

 inline bool jj_3_1106()
 {
    if (jj_done) return true;
    if (jj_scan_token(NOT)) return true;
    return false;
  }

 inline bool jj_3R_regex_like_predicate_part_2_3674_5_238()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1106()) jj_scanpos = xsp;
    if (jj_scan_token(LIKE_REGEX)) return true;
    if (jj_3R_character_value_expression_2128_5_274()) return true;
    xsp = jj_scanpos;
    if (jj_3_1107()) jj_scanpos = xsp;
    return false;
  }

 inline bool jj_3R_regex_like_predicate_3668_5_448()
 {
    if (jj_done) return true;
    if (jj_3R_regex_like_predicate_part_2_3674_5_238()) return true;
    return false;
  }

 inline bool jj_3_1104()
 {
    if (jj_done) return true;
    if (jj_scan_token(NOT)) return true;
    return false;
  }

 inline bool jj_3R_similar_predicate_part_2_3662_5_237()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1104()) jj_scanpos = xsp;
    if (jj_scan_token(SIMILAR)) return true;
    if (jj_scan_token(TO)) return true;
    if (jj_3R_character_value_expression_2128_5_274()) return true;
    xsp = jj_scanpos;
    if (jj_3_1105()) jj_scanpos = xsp;
    return false;
  }

 inline bool jj_3R_similar_predicate_3656_5_447()
 {
    if (jj_done) return true;
    if (jj_3R_similar_predicate_part_2_3662_5_237()) return true;
    return false;
  }

 inline bool jj_3_1102()
 {
    if (jj_done) return true;
    if (jj_scan_token(NOT)) return true;
    return false;
  }

 inline bool jj_3R_octet_like_predicate_part_2_3650_5_236()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1102()) jj_scanpos = xsp;
    if (jj_scan_token(LIKE)) return true;
    if (jj_3R_binary_value_expression_2154_5_298()) return true;
    xsp = jj_scanpos;
    if (jj_3_1103()) jj_scanpos = xsp;
    return false;
  }

 inline bool jj_3R_octet_like_predicate_3644_5_463()
 {
    if (jj_done) return true;
    if (jj_3R_octet_like_predicate_part_2_3650_5_236()) return true;
    return false;
  }

 inline bool jj_3_1097()
 {
    if (jj_done) return true;
    if (jj_scan_token(570)) return true;
    if (jj_3R_row_value_expression_2737_5_260()) return true;
    return false;
  }

 inline bool jj_3_1100()
 {
    if (jj_done) return true;
    if (jj_scan_token(NOT)) return true;
    return false;
  }

 inline bool jj_3R_character_like_predicate_part_2_3638_5_235()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1100()) jj_scanpos = xsp;
    if (jj_scan_token(LIKE)) return true;
    if (jj_3R_character_value_expression_2128_5_274()) return true;
    xsp = jj_scanpos;
    if (jj_3_1101()) jj_scanpos = xsp;
    return false;
  }

 inline bool jj_3R_character_like_predicate_3632_5_462()
 {
    if (jj_done) return true;
    if (jj_3R_character_like_predicate_part_2_3638_5_235()) return true;
    return false;
  }

 inline bool jj_3_1092()
 {
    if (jj_done) return true;
    if (jj_scan_token(SYMMETRIC)) return true;
    return false;
  }

 inline bool jj_3_1099()
 {
    if (jj_done) return true;
    if (jj_3R_octet_like_predicate_3644_5_463()) return true;
    return false;
  }

 inline bool jj_3R_like_predicate_3625_5_446()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1098()) {
    jj_scanpos = xsp;
    if (jj_3_1099()) return true;
    }
    return false;
  }

 inline bool jj_3_1098()
 {
    if (jj_done) return true;
    if (jj_3R_character_like_predicate_3632_5_462()) return true;
    return false;
  }

 inline bool jj_3R_in_value_list_3619_5_461()
 {
    if (jj_done) return true;
    if (jj_3R_row_value_expression_2737_5_260()) return true;
    Token * xsp;
    while (true) {
      xsp = jj_scanpos;
      if (jj_3_1097()) { jj_scanpos = xsp; break; }
    }
    return false;
  }

 inline bool jj_3_1093()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1091()) {
    jj_scanpos = xsp;
    if (jj_3_1092()) return true;
    }
    return false;
  }

 inline bool jj_3_1091()
 {
    if (jj_done) return true;
    if (jj_scan_token(ASYMMETRIC)) return true;
    return false;
  }

 inline bool jj_3_1096()
 {
    if (jj_done) return true;
    if (jj_3R_subquery_3527_5_181()) return true;
    return false;
  }

 inline bool jj_3R_in_predicate_value_3612_5_904()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1095()) {
    jj_scanpos = xsp;
    if (jj_3_1096()) return true;
    }
    return false;
  }

 inline bool jj_3_1095()
 {
    if (jj_done) return true;
    if (jj_scan_token(lparen)) return true;
    if (jj_3R_in_value_list_3619_5_461()) return true;
    if (jj_scan_token(rparen)) return true;
    return false;
  }

 inline bool jj_3_1094()
 {
    if (jj_done) return true;
    if (jj_scan_token(NOT)) return true;
    return false;
  }

 inline bool jj_3R_in_predicate_part_2_3606_5_234()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1094()) jj_scanpos = xsp;
    if (jj_scan_token(IN)) return true;
    if (jj_3R_in_predicate_value_3612_5_904()) return true;
    return false;
  }

 inline bool jj_3R_in_predicate_3600_5_445()
 {
    if (jj_done) return true;
    if (jj_3R_in_predicate_part_2_3606_5_234()) return true;
    return false;
  }

 inline bool jj_3_1090()
 {
    if (jj_done) return true;
    if (jj_scan_token(NOT)) return true;
    return false;
  }

 inline bool jj_3R_between_predicate_part_2_3593_5_233()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1090()) jj_scanpos = xsp;
    if (jj_scan_token(BETWEEN)) return true;
    xsp = jj_scanpos;
    if (jj_3_1093()) jj_scanpos = xsp;
    if (jj_3R_row_value_predicand_2758_5_229()) return true;
    if (jj_scan_token(AND)) return true;
    if (jj_3R_row_value_predicand_2758_5_229()) return true;
    return false;
  }

 inline bool jj_3R_between_predicate_3587_5_444()
 {
    if (jj_done) return true;
    if (jj_3R_between_predicate_part_2_3593_5_233()) return true;
    return false;
  }

 inline bool jj_3_1089()
 {
    if (jj_done) return true;
    if (jj_scan_token(NOT_EQUAL_2)) return true;
    return false;
  }

 inline bool jj_3_1088()
 {
    if (jj_done) return true;
    if (jj_scan_token(GREATER_THAN_OR_EQUAL)) return true;
    return false;
  }

 inline bool jj_3_1087()
 {
    if (jj_done) return true;
    if (jj_scan_token(LESS_THAN_OR_EQUAL)) return true;
    return false;
  }

 inline bool jj_3_1086()
 {
    if (jj_done) return true;
    if (jj_scan_token(GREATER_THAN)) return true;
    return false;
  }

 inline bool jj_3_1085()
 {
    if (jj_done) return true;
    if (jj_scan_token(LESS_THAN)) return true;
    return false;
  }

 inline bool jj_3_1084()
 {
    if (jj_done) return true;
    if (jj_scan_token(NOT_EQUAL)) return true;
    return false;
  }

 inline bool jj_3R_comp_op_3575_5_903()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1083()) {
    jj_scanpos = xsp;
    if (jj_3_1084()) {
    jj_scanpos = xsp;
    if (jj_3_1085()) {
    jj_scanpos = xsp;
    if (jj_3_1086()) {
    jj_scanpos = xsp;
    if (jj_3_1087()) {
    jj_scanpos = xsp;
    if (jj_3_1088()) {
    jj_scanpos = xsp;
    if (jj_3_1089()) return true;
    }
    }
    }
    }
    }
    }
    return false;
  }

 inline bool jj_3_1083()
 {
    if (jj_done) return true;
    if (jj_scan_token(EQUAL)) return true;
    return false;
  }

 inline bool jj_3R_comparison_predicate_part_2_3569_5_232()
 {
    if (jj_done) return true;
    if (jj_3R_comp_op_3575_5_903()) return true;
    if (jj_3R_row_value_predicand_2758_5_229()) return true;
    return false;
  }

 inline bool jj_3R_comparison_predicate_3563_5_443()
 {
    if (jj_done) return true;
    if (jj_3R_comparison_predicate_part_2_3569_5_232()) return true;
    return false;
  }

 inline bool jj_3_1078()
 {
    if (jj_done) return true;
    if (jj_3R_type_predicate_3830_5_458()) return true;
    return false;
  }

 inline bool jj_3_1056()
 {
    if (jj_done) return true;
    if (jj_scan_token(ROWS)) return true;
    return false;
  }

 inline bool jj_3_1077()
 {
    if (jj_done) return true;
    if (jj_3R_set_predicate_3818_5_457()) return true;
    return false;
  }

 inline bool jj_3_1076()
 {
    if (jj_done) return true;
    if (jj_3R_submultiset_predicate_3806_5_456()) return true;
    return false;
  }

 inline bool jj_3_1075()
 {
    if (jj_done) return true;
    if (jj_3R_member_predicate_3794_5_455()) return true;
    return false;
  }

 inline bool jj_3_1074()
 {
    if (jj_done) return true;
    if (jj_3R_distinct_predicate_3770_5_454()) return true;
    return false;
  }

 inline bool jj_3_1073()
 {
    if (jj_done) return true;
    if (jj_3R_overlaps_predicate_3740_5_453()) return true;
    return false;
  }

 inline bool jj_3_1072()
 {
    if (jj_done) return true;
    if (jj_3R_match_predicate_3728_5_452()) return true;
    return false;
  }

 inline bool jj_3_1071()
 {
    if (jj_done) return true;
    if (jj_3R_normalized_predicate_3716_5_451()) return true;
    return false;
  }

 inline bool jj_3_1070()
 {
    if (jj_done) return true;
    if (jj_3R_quantified_comparison_predicate_3692_5_450()) return true;
    return false;
  }

 inline bool jj_3_1055()
 {
    if (jj_done) return true;
    if (jj_scan_token(ROW)) return true;
    return false;
  }

 inline bool jj_3_1069()
 {
    if (jj_done) return true;
    if (jj_3R_null_predicate_3680_5_449()) return true;
    return false;
  }

 inline bool jj_3_1068()
 {
    if (jj_done) return true;
    if (jj_3R_regex_like_predicate_3668_5_448()) return true;
    return false;
  }

 inline bool jj_3_1067()
 {
    if (jj_done) return true;
    if (jj_3R_similar_predicate_3656_5_447()) return true;
    return false;
  }

 inline bool jj_3_1066()
 {
    if (jj_done) return true;
    if (jj_3R_like_predicate_3625_5_446()) return true;
    return false;
  }

 inline bool jj_3_1065()
 {
    if (jj_done) return true;
    if (jj_3R_in_predicate_3600_5_445()) return true;
    return false;
  }

 inline bool jj_3_1064()
 {
    if (jj_done) return true;
    if (jj_3R_between_predicate_3587_5_444()) return true;
    return false;
  }

 inline bool jj_3_1063()
 {
    if (jj_done) return true;
    if (jj_3R_comparison_predicate_3563_5_443()) return true;
    return false;
  }

 inline bool jj_3_1079()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1063()) {
    jj_scanpos = xsp;
    if (jj_3_1064()) {
    jj_scanpos = xsp;
    if (jj_3_1065()) {
    jj_scanpos = xsp;
    if (jj_3_1066()) {
    jj_scanpos = xsp;
    if (jj_3_1067()) {
    jj_scanpos = xsp;
    if (jj_3_1068()) {
    jj_scanpos = xsp;
    if (jj_3_1069()) {
    jj_scanpos = xsp;
    if (jj_3_1070()) {
    jj_scanpos = xsp;
    if (jj_3_1071()) {
    jj_scanpos = xsp;
    if (jj_3_1072()) {
    jj_scanpos = xsp;
    if (jj_3_1073()) {
    jj_scanpos = xsp;
    if (jj_3_1074()) {
    jj_scanpos = xsp;
    if (jj_3_1075()) {
    jj_scanpos = xsp;
    if (jj_3_1076()) {
    jj_scanpos = xsp;
    if (jj_3_1077()) {
    jj_scanpos = xsp;
    if (jj_3_1078()) return true;
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    return false;
  }

 inline bool jj_3_1062()
 {
    if (jj_done) return true;
    if (jj_scan_token(570)) return true;
    if (jj_3R_identifier_928_3_141()) return true;
    return false;
  }

 inline bool jj_3_1082()
 {
    if (jj_done) return true;
    if (jj_3R_row_value_predicand_2758_5_229()) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1079()) jj_scanpos = xsp;
    return false;
  }

 inline bool jj_3_1081()
 {
    if (jj_done) return true;
    if (jj_3R_unique_predicate_3710_5_460()) return true;
    return false;
  }

 inline bool jj_3R_predicate_3533_5_337()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1080()) {
    jj_scanpos = xsp;
    if (jj_3_1081()) {
    jj_scanpos = xsp;
    if (jj_3_1082()) return true;
    }
    }
    return false;
  }

 inline bool jj_3_1080()
 {
    if (jj_done) return true;
    if (jj_3R_exists_predicate_3704_5_459()) return true;
    return false;
  }

 inline bool jj_3_1051()
 {
    if (jj_done) return true;
    if (jj_scan_token(ROWS)) return true;
    return false;
  }

 inline bool jj_3R_subquery_3527_5_181()
 {
    if (jj_done) return true;
    if (jj_scan_token(lparen)) return true;
    if (jj_3R_query_expression_3399_5_547()) return true;
    if (jj_scan_token(rparen)) return true;
    return false;
  }

 inline bool jj_3R_cycle_column_list_3521_5_942()
 {
    if (jj_done) return true;
    if (jj_3R_identifier_928_3_141()) return true;
    Token * xsp;
    while (true) {
      xsp = jj_scanpos;
      if (jj_3_1062()) { jj_scanpos = xsp; break; }
    }
    return false;
  }

 inline bool jj_3_1050()
 {
    if (jj_done) return true;
    if (jj_scan_token(ROW)) return true;
    return false;
  }

 inline bool jj_3_1054()
 {
    if (jj_done) return true;
    if (jj_3R_simple_value_specification_1423_5_220()) return true;
    return false;
  }

 inline bool jj_3R_cycle_clause_3514_5_442()
 {
    if (jj_done) return true;
    if (jj_scan_token(CYCLE)) return true;
    if (jj_3R_cycle_column_list_3521_5_942()) return true;
    if (jj_scan_token(SET)) return true;
    if (jj_3R_identifier_928_3_141()) return true;
    if (jj_scan_token(TO)) return true;
    if (jj_3R_value_expression_1855_5_178()) return true;
    if (jj_scan_token(DEFAULT_)) return true;
    if (jj_3R_value_expression_1855_5_178()) return true;
    if (jj_scan_token(USING)) return true;
    if (jj_3R_identifier_928_3_141()) return true;
    return false;
  }

 inline bool jj_3_1061()
 {
    if (jj_done) return true;
    if (jj_scan_token(BREADTH)) return true;
    if (jj_scan_token(FIRST)) return true;
    if (jj_scan_token(BY)) return true;
    if (jj_3R_column_name_list_2942_5_191()) return true;
    return false;
  }

 inline bool jj_3R_recursive_search_order_3507_5_941()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1060()) {
    jj_scanpos = xsp;
    if (jj_3_1061()) return true;
    }
    return false;
  }

 inline bool jj_3_1060()
 {
    if (jj_done) return true;
    if (jj_scan_token(DEPTH)) return true;
    if (jj_scan_token(FIRST)) return true;
    if (jj_scan_token(BY)) return true;
    if (jj_3R_column_name_list_2942_5_191()) return true;
    return false;
  }

 inline bool jj_3_1053()
 {
    if (jj_done) return true;
    if (jj_scan_token(NEXT)) return true;
    return false;
  }

 inline bool jj_3R_search_clause_3501_5_441()
 {
    if (jj_done) return true;
    if (jj_scan_token(SEARCH)) return true;
    if (jj_3R_recursive_search_order_3507_5_941()) return true;
    if (jj_scan_token(SET)) return true;
    if (jj_3R_identifier_928_3_141()) return true;
    return false;
  }

 inline bool jj_3_1043()
 {
    if (jj_done) return true;
    if (jj_3R_fetch_first_clause_3487_5_428()) return true;
    return false;
  }

 inline bool jj_3_1052()
 {
    if (jj_done) return true;
    if (jj_scan_token(FIRST)) return true;
    return false;
  }

 inline bool jj_3_1059()
 {
    if (jj_done) return true;
    if (jj_3R_search_clause_3501_5_441()) return true;
    if (jj_3R_cycle_clause_3514_5_442()) return true;
    return false;
  }

 inline bool jj_3_1058()
 {
    if (jj_done) return true;
    if (jj_3R_cycle_clause_3514_5_442()) return true;
    return false;
  }

 inline bool jj_3R_search_or_cycle_clause_3493_5_432()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1057()) {
    jj_scanpos = xsp;
    if (jj_3_1058()) {
    jj_scanpos = xsp;
    if (jj_3_1059()) return true;
    }
    }
    return false;
  }

 inline bool jj_3_1057()
 {
    if (jj_done) return true;
    if (jj_3R_search_clause_3501_5_441()) return true;
    return false;
  }

 inline bool jj_3_1039()
 {
    if (jj_done) return true;
    if (jj_3R_corresponding_spec_3469_5_433()) return true;
    return false;
  }

 inline bool jj_3R_fetch_first_clause_3487_5_428()
 {
    if (jj_done) return true;
    if (jj_scan_token(FETCH)) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1052()) {
    jj_scanpos = xsp;
    if (jj_3_1053()) return true;
    }
    xsp = jj_scanpos;
    if (jj_3_1054()) jj_scanpos = xsp;
    xsp = jj_scanpos;
    if (jj_3_1055()) {
    jj_scanpos = xsp;
    if (jj_3_1056()) return true;
    }
    if (jj_scan_token(ONLY)) return true;
    return false;
  }

 inline bool jj_3_1049()
 {
    if (jj_done) return true;
    if (jj_scan_token(BY)) return true;
    if (jj_scan_token(lparen)) return true;
    if (jj_3R_column_name_list_2942_5_191()) return true;
    if (jj_scan_token(rparen)) return true;
    return false;
  }

 inline bool jj_3R_result_offset_clause_3481_5_430()
 {
    if (jj_done) return true;
    if (jj_scan_token(OFFSET)) return true;
    if (jj_3R_simple_value_specification_1423_5_220()) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1050()) {
    jj_scanpos = xsp;
    if (jj_3_1051()) return true;
    }
    return false;
  }

 inline bool jj_3_1037()
 {
    if (jj_done) return true;
    if (jj_scan_token(DISTINCT)) return true;
    return false;
  }

 inline bool jj_3R_order_by_clause_3475_5_427()
 {
    if (jj_done) return true;
    if (jj_scan_token(ORDER)) return true;
    if (jj_scan_token(BY)) return true;
    if (jj_3R_sort_specification_list_4266_5_495()) return true;
    return false;
  }

 inline bool jj_3_1042()
 {
    if (jj_done) return true;
    if (jj_3R_result_offset_clause_3481_5_430()) return true;
    return false;
  }

 inline bool jj_3_1018()
 {
    if (jj_done) return true;
    if (jj_3R_fetch_first_clause_3487_5_428()) return true;
    return false;
  }

 inline bool jj_3R_query_primary_3446_30_1047()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1042()) jj_scanpos = xsp;
    xsp = jj_scanpos;
    if (jj_3_1043()) jj_scanpos = xsp;
    return false;
  }

 inline bool jj_3_1038()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1036()) {
    jj_scanpos = xsp;
    if (jj_3_1037()) return true;
    }
    return false;
  }

 inline bool jj_3_1036()
 {
    if (jj_done) return true;
    if (jj_scan_token(ALL)) return true;
    return false;
  }

 inline bool jj_3R_corresponding_spec_3469_5_433()
 {
    if (jj_done) return true;
    if (jj_scan_token(CORRESPONDING)) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1049()) jj_scanpos = xsp;
    return false;
  }

 inline bool jj_3R_explicit_table_3463_5_439()
 {
    if (jj_done) return true;
    if (jj_scan_token(TABLE)) return true;
    if (jj_3R_table_or_query_name_2935_5_374()) return true;
    return false;
  }

 inline bool jj_3_1048()
 {
    if (jj_done) return true;
    if (jj_3R_query_specification_3323_5_440()) return true;
    return false;
  }

 inline bool jj_3_1047()
 {
    if (jj_done) return true;
    if (jj_3R_explicit_table_3463_5_439()) return true;
    return false;
  }

 inline bool jj_3_1035()
 {
    if (jj_done) return true;
    if (jj_scan_token(INTERSECT)) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1038()) jj_scanpos = xsp;
    xsp = jj_scanpos;
    if (jj_3_1039()) jj_scanpos = xsp;
    if (jj_3R_query_primary_3444_5_435()) return true;
    return false;
  }

 inline bool jj_3R_simple_table_3455_5_437()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1046()) {
    jj_scanpos = xsp;
    if (jj_3_1047()) {
    jj_scanpos = xsp;
    if (jj_3_1048()) return true;
    }
    }
    return false;
  }

 inline bool jj_3_1046()
 {
    if (jj_done) return true;
    if (jj_3R_table_value_constructor_2772_5_438()) return true;
    return false;
  }

 inline bool jj_3_1041()
 {
    if (jj_done) return true;
    if (jj_3R_limit_clause_7950_5_429()) return true;
    return false;
  }

 inline bool jj_3_1030()
 {
    if (jj_done) return true;
    if (jj_scan_token(DISTINCT)) return true;
    return false;
  }

 inline bool jj_3R_query_primary_3446_9_1046()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1041()) jj_scanpos = xsp;
    return false;
  }

 inline bool jj_3_1045()
 {
    if (jj_done) return true;
    if (jj_3R_simple_table_3455_5_437()) return true;
    return false;
  }

 inline bool jj_3_1040()
 {
    if (jj_done) return true;
    if (jj_3R_order_by_clause_3475_5_427()) return true;
    return false;
  }

 inline bool jj_3_1026()
 {
    if (jj_done) return true;
    if (jj_scan_token(DISTINCT)) return true;
    return false;
  }

 inline bool jj_3_1015()
 {
    if (jj_done) return true;
    if (jj_3R_fetch_first_clause_3487_5_428()) return true;
    return false;
  }

 inline bool jj_3R_query_primary_3444_5_435()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1044()) {
    jj_scanpos = xsp;
    if (jj_3_1045()) return true;
    }
    return false;
  }

 inline bool jj_3_1044()
 {
    if (jj_done) return true;
    if (jj_scan_token(lparen)) return true;
    if (jj_3R_query_expression_body_3427_5_436()) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1040()) jj_scanpos = xsp;
    xsp = jj_scanpos;
    if (jj_3R_query_primary_3446_9_1046()) {
    jj_scanpos = xsp;
    if (jj_3R_query_primary_3446_30_1047()) return true;
    }
    if (jj_scan_token(rparen)) return true;
    return false;
  }

 inline bool jj_3_1031()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1029()) {
    jj_scanpos = xsp;
    if (jj_3_1030()) return true;
    }
    return false;
  }

 inline bool jj_3_1029()
 {
    if (jj_done) return true;
    if (jj_scan_token(ALL)) return true;
    return false;
  }

 inline bool jj_3_1027()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1025()) {
    jj_scanpos = xsp;
    if (jj_3_1026()) return true;
    }
    return false;
  }

 inline bool jj_3_1025()
 {
    if (jj_done) return true;
    if (jj_scan_token(ALL)) return true;
    return false;
  }

 inline bool jj_3_1023()
 {
    if (jj_done) return true;
    if (jj_3R_search_or_cycle_clause_3493_5_432()) return true;
    return false;
  }

 inline bool jj_3R_query_term_3438_5_434()
 {
    if (jj_done) return true;
    if (jj_3R_query_primary_3444_5_435()) return true;
    Token * xsp;
    while (true) {
      xsp = jj_scanpos;
      if (jj_3_1035()) { jj_scanpos = xsp; break; }
    }
    return false;
  }

 inline bool jj_3_1021()
 {
    if (jj_done) return true;
    if (jj_scan_token(570)) return true;
    if (jj_3R_with_list_element_3420_5_431()) return true;
    return false;
  }

 inline bool jj_3_1032()
 {
    if (jj_done) return true;
    if (jj_3R_corresponding_spec_3469_5_433()) return true;
    return false;
  }

 inline bool jj_3_1022()
 {
    if (jj_done) return true;
    if (jj_scan_token(lparen)) return true;
    if (jj_3R_column_name_list_2942_5_191()) return true;
    if (jj_scan_token(rparen)) return true;
    return false;
  }

 inline bool jj_3_1034()
 {
    if (jj_done) return true;
    if (jj_scan_token(EXCEPT)) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1031()) jj_scanpos = xsp;
    xsp = jj_scanpos;
    if (jj_3_1032()) jj_scanpos = xsp;
    if (jj_3R_query_term_3438_5_434()) return true;
    return false;
  }

 inline bool jj_3_1028()
 {
    if (jj_done) return true;
    if (jj_3R_corresponding_spec_3469_5_433()) return true;
    return false;
  }

 inline bool jj_3_1033()
 {
    if (jj_done) return true;
    if (jj_scan_token(UNION)) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1027()) jj_scanpos = xsp;
    xsp = jj_scanpos;
    if (jj_3_1028()) jj_scanpos = xsp;
    if (jj_3R_query_term_3438_5_434()) return true;
    return false;
  }

 inline bool jj_3_1024()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1033()) {
    jj_scanpos = xsp;
    if (jj_3_1034()) return true;
    }
    return false;
  }

 inline bool jj_3R_query_expression_body_3427_5_436()
 {
    if (jj_done) return true;
    if (jj_3R_query_term_3438_5_434()) return true;
    Token * xsp;
    while (true) {
      xsp = jj_scanpos;
      if (jj_3_1024()) { jj_scanpos = xsp; break; }
    }
    return false;
  }

 inline bool jj_3_1017()
 {
    if (jj_done) return true;
    if (jj_3R_result_offset_clause_3481_5_430()) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1015()) jj_scanpos = xsp;
    return false;
  }

 inline bool jj_3R_with_list_element_3420_5_431()
 {
    if (jj_done) return true;
    if (jj_3R_identifier_928_3_141()) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1022()) jj_scanpos = xsp;
    if (jj_scan_token(AS)) return true;
    if (jj_3R_subquery_3527_5_181()) return true;
    xsp = jj_scanpos;
    if (jj_3_1023()) jj_scanpos = xsp;
    return false;
  }

 inline bool jj_3_1020()
 {
    if (jj_done) return true;
    if (jj_scan_token(RECURSIVE)) return true;
    return false;
  }

 inline bool jj_3R_with_list_3414_5_938()
 {
    if (jj_done) return true;
    if (jj_3R_with_list_element_3420_5_431()) return true;
    Token * xsp;
    while (true) {
      xsp = jj_scanpos;
      if (jj_3_1021()) { jj_scanpos = xsp; break; }
    }
    return false;
  }

 inline bool jj_3R_with_clause_3408_5_426()
 {
    if (jj_done) return true;
    if (jj_scan_token(WITH)) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1020()) jj_scanpos = xsp;
    if (jj_3R_with_list_3414_5_938()) return true;
    return false;
  }

 inline bool jj_3_1019()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1016()) {
    jj_scanpos = xsp;
    if (jj_3_1017()) {
    jj_scanpos = xsp;
    if (jj_3_1018()) return true;
    }
    }
    return false;
  }

 inline bool jj_3_1016()
 {
    if (jj_done) return true;
    if (jj_3R_limit_clause_7950_5_429()) return true;
    return false;
  }

 inline bool jj_3_1014()
 {
    if (jj_done) return true;
    if (jj_3R_order_by_clause_3475_5_427()) return true;
    return false;
  }

 inline bool jj_3_1013()
 {
    if (jj_done) return true;
    if (jj_3R_with_clause_3408_5_426()) return true;
    return false;
  }

 inline bool jj_3R_query_expression_3399_5_547()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1013()) jj_scanpos = xsp;
    if (jj_3R_query_expression_body_3427_5_436()) return true;
    xsp = jj_scanpos;
    if (jj_3_1014()) jj_scanpos = xsp;
    xsp = jj_scanpos;
    if (jj_3_1019()) jj_scanpos = xsp;
    return false;
  }

 inline bool jj_3_1010()
 {
    if (jj_done) return true;
    if (jj_3R_as_clause_3380_5_423()) return true;
    return false;
  }

 inline bool jj_3R_all_fields_column_name_list_3393_5_422()
 {
    if (jj_done) return true;
    if (jj_3R_column_name_list_2942_5_191()) return true;
    return false;
  }

 inline bool jj_3_1012()
 {
    if (jj_done) return true;
    if (jj_scan_token(AS)) return true;
    if (jj_scan_token(lparen)) return true;
    if (jj_3R_all_fields_column_name_list_3393_5_422()) return true;
    return false;
  }

 inline bool jj_3R_all_fields_reference_3386_5_425()
 {
    if (jj_done) return true;
    if (jj_3R_value_expression_primary_1315_5_349()) return true;
    if (jj_scan_token(569)) return true;
    if (jj_scan_token(STAR)) return true;
    return false;
  }

 inline bool jj_3_1009()
 {
    if (jj_done) return true;
    if (jj_scan_token(569)) return true;
    if (jj_3R_identifier_928_3_141()) return true;
    return false;
  }

 inline bool jj_3_1011()
 {
    if (jj_done) return true;
    if (jj_scan_token(AS)) return true;
    return false;
  }

 inline bool jj_3R_as_clause_3380_5_423()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1011()) jj_scanpos = xsp;
    if (jj_3R_identifier_928_3_141()) return true;
    return false;
  }

 inline bool jj_3R_derived_column_3374_5_937()
 {
    if (jj_done) return true;
    if (jj_3R_value_expression_1855_5_178()) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1010()) jj_scanpos = xsp;
    return false;
  }

 inline bool jj_3R_asterisked_identifier_chain_3368_5_424()
 {
    if (jj_done) return true;
    if (jj_3R_identifier_928_3_141()) return true;
    Token * xsp;
    while (true) {
      xsp = jj_scanpos;
      if (jj_3_1009()) { jj_scanpos = xsp; break; }
    }
    return false;
  }

 inline bool jj_3_1008()
 {
    if (jj_done) return true;
    if (jj_3R_all_fields_reference_3386_5_425()) return true;
    return false;
  }

 inline bool jj_3_1007()
 {
    if (jj_done) return true;
    if (jj_3R_asterisked_identifier_chain_3368_5_424()) return true;
    if (jj_scan_token(569)) return true;
    if (jj_scan_token(STAR)) return true;
    return false;
  }

 inline bool jj_3_1003()
 {
    if (jj_done) return true;
    if (jj_scan_token(AS)) return true;
    if (jj_scan_token(lparen)) return true;
    if (jj_3R_all_fields_column_name_list_3393_5_422()) return true;
    if (jj_scan_token(rparen)) return true;
    return false;
  }

 inline bool jj_3_1005()
 {
    if (jj_done) return true;
    if (jj_3R_as_clause_3380_5_423()) return true;
    return false;
  }

 inline bool jj_3_1006()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1004()) {
    jj_scanpos = xsp;
    if (jj_3_1005()) return true;
    }
    return false;
  }

 inline bool jj_3_1004()
 {
    if (jj_done) return true;
    if (jj_scan_token(569)) return true;
    if (jj_scan_token(STAR)) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1003()) jj_scanpos = xsp;
    return false;
  }

 inline bool jj_3_998()
 {
    if (jj_done) return true;
    if (jj_scan_token(570)) return true;
    if (jj_3R_select_sublist_3346_5_420()) return true;
    return false;
  }

 inline bool jj_3R_select_sublist_3346_5_420()
 {
    if (jj_done) return true;
    if (jj_3R_derived_column_3374_5_937()) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1006()) jj_scanpos = xsp;
    return false;
  }

 inline bool jj_3R_star_3340_5_421()
 {
    if (jj_done) return true;
    if (jj_scan_token(STAR)) return true;
    return false;
  }

 inline bool jj_3_1000()
 {
    if (jj_done) return true;
    if (jj_scan_token(570)) return true;
    if (jj_3R_select_sublist_3346_5_420()) return true;
    return false;
  }

 inline bool jj_3_999()
 {
    if (jj_done) return true;
    if (jj_scan_token(570)) return true;
    if (jj_3R_star_3340_5_421()) return true;
    return false;
  }

 inline bool jj_3_995()
 {
    if (jj_done) return true;
    if (jj_3R_set_quantifier_4159_5_396()) return true;
    return false;
  }

 inline bool jj_3_997()
 {
    if (jj_done) return true;
    if (jj_scan_token(570)) return true;
    if (jj_3R_select_sublist_3346_5_420()) return true;
    return false;
  }

 inline bool jj_3_1002()
 {
    if (jj_done) return true;
    if (jj_3R_select_sublist_3346_5_420()) return true;
    Token * xsp;
    while (true) {
      xsp = jj_scanpos;
      if (jj_3_998()) { jj_scanpos = xsp; break; }
    }
    xsp = jj_scanpos;
    if (jj_3_999()) jj_scanpos = xsp;
    while (true) {
      xsp = jj_scanpos;
      if (jj_3_1000()) { jj_scanpos = xsp; break; }
    }
    return false;
  }

 inline bool jj_3R_select_list_3330_5_940()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1001()) {
    jj_scanpos = xsp;
    if (jj_3_1002()) return true;
    }
    return false;
  }

 inline bool jj_3_1001()
 {
    if (jj_done) return true;
    if (jj_3R_star_3340_5_421()) return true;
    Token * xsp;
    while (true) {
      xsp = jj_scanpos;
      if (jj_3_997()) { jj_scanpos = xsp; break; }
    }
    return false;
  }

 inline bool jj_3_996()
 {
    if (jj_done) return true;
    if (jj_3R_table_expression_2797_5_419()) return true;
    return false;
  }

 inline bool jj_3R_query_specification_3323_5_440()
 {
    if (jj_done) return true;
    if (jj_scan_token(SELECT)) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_995()) jj_scanpos = xsp;
    if (jj_3R_select_list_3330_5_940()) return true;
    xsp = jj_scanpos;
    if (jj_3_996()) jj_scanpos = xsp;
    return false;
  }

 inline bool jj_3_994()
 {
    if (jj_done) return true;
    if (jj_scan_token(EXCLUDE)) return true;
    if (jj_scan_token(NO)) return true;
    if (jj_scan_token(OTHERS)) return true;
    return false;
  }

 inline bool jj_3_993()
 {
    if (jj_done) return true;
    if (jj_scan_token(EXCLUDE)) return true;
    if (jj_scan_token(TIES)) return true;
    return false;
  }

 inline bool jj_3_992()
 {
    if (jj_done) return true;
    if (jj_scan_token(EXCLUDE)) return true;
    if (jj_scan_token(GROUP)) return true;
    return false;
  }

 inline bool jj_3R_window_frame_exclusion_3314_5_414()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_991()) {
    jj_scanpos = xsp;
    if (jj_3_992()) {
    jj_scanpos = xsp;
    if (jj_3_993()) {
    jj_scanpos = xsp;
    if (jj_3_994()) return true;
    }
    }
    }
    return false;
  }

 inline bool jj_3_991()
 {
    if (jj_done) return true;
    if (jj_scan_token(EXCLUDE)) return true;
    if (jj_scan_token(CURRENT)) return true;
    if (jj_scan_token(ROW)) return true;
    return false;
  }

 inline bool jj_3R_window_frame_following_3307_5_418()
 {
    if (jj_done) return true;
    if (jj_3R_value_expression_1855_5_178()) return true;
    if (jj_scan_token(FOLLOWING)) return true;
    return false;
  }

 inline bool jj_3_990()
 {
    if (jj_done) return true;
    if (jj_3R_window_frame_following_3307_5_418()) return true;
    return false;
  }

 inline bool jj_3_989()
 {
    if (jj_done) return true;
    if (jj_scan_token(UNBOUNDED)) return true;
    if (jj_scan_token(FOLLOWING)) return true;
    return false;
  }

 inline bool jj_3R_window_frame_bound_3299_5_935()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_988()) {
    jj_scanpos = xsp;
    if (jj_3_989()) {
    jj_scanpos = xsp;
    if (jj_3_990()) return true;
    }
    }
    return false;
  }

 inline bool jj_3_988()
 {
    if (jj_done) return true;
    if (jj_3R_window_frame_start_3278_5_415()) return true;
    return false;
  }

 inline bool jj_3R_window_frame_between_3293_5_416()
 {
    if (jj_done) return true;
    if (jj_scan_token(BETWEEN)) return true;
    if (jj_3R_window_frame_bound_3299_5_935()) return true;
    if (jj_scan_token(AND)) return true;
    if (jj_3R_window_frame_bound_3299_5_935()) return true;
    return false;
  }

 inline bool jj_3R_window_frame_preceding_3286_5_417()
 {
    if (jj_done) return true;
    if (jj_3R_value_expression_1855_5_178()) return true;
    if (jj_scan_token(PRECEDING)) return true;
    return false;
  }

 inline bool jj_3_987()
 {
    if (jj_done) return true;
    if (jj_3R_window_frame_preceding_3286_5_417()) return true;
    return false;
  }

 inline bool jj_3_986()
 {
    if (jj_done) return true;
    if (jj_scan_token(CURRENT)) return true;
    if (jj_scan_token(ROW)) return true;
    return false;
  }

 inline bool jj_3R_window_frame_start_3278_5_415()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_985()) {
    jj_scanpos = xsp;
    if (jj_3_986()) {
    jj_scanpos = xsp;
    if (jj_3_987()) return true;
    }
    }
    return false;
  }

 inline bool jj_3_985()
 {
    if (jj_done) return true;
    if (jj_scan_token(UNBOUNDED)) return true;
    if (jj_scan_token(PRECEDING)) return true;
    return false;
  }

 inline bool jj_3_984()
 {
    if (jj_done) return true;
    if (jj_3R_window_frame_between_3293_5_416()) return true;
    return false;
  }

 inline bool jj_3R_window_frame_extent_3271_5_934()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_983()) {
    jj_scanpos = xsp;
    if (jj_3_984()) return true;
    }
    return false;
  }

 inline bool jj_3_983()
 {
    if (jj_done) return true;
    if (jj_3R_window_frame_start_3278_5_415()) return true;
    return false;
  }

 inline bool jj_3_982()
 {
    if (jj_done) return true;
    if (jj_scan_token(RANGE)) return true;
    return false;
  }

 inline bool jj_3R_window_frame_units_3264_5_933()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_981()) {
    jj_scanpos = xsp;
    if (jj_3_982()) return true;
    }
    return false;
  }

 inline bool jj_3_981()
 {
    if (jj_done) return true;
    if (jj_scan_token(ROWS)) return true;
    return false;
  }

 inline bool jj_3_980()
 {
    if (jj_done) return true;
    if (jj_3R_window_frame_exclusion_3314_5_414()) return true;
    return false;
  }

 inline bool jj_3R_window_frame_clause_3257_5_411()
 {
    if (jj_done) return true;
    if (jj_3R_window_frame_units_3264_5_933()) return true;
    if (jj_3R_window_frame_extent_3271_5_934()) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_980()) jj_scanpos = xsp;
    return false;
  }

 inline bool jj_3R_window_order_clause_3251_5_410()
 {
    if (jj_done) return true;
    if (jj_scan_token(ORDER)) return true;
    if (jj_scan_token(BY)) return true;
    if (jj_3R_sort_specification_list_4266_5_495()) return true;
    return false;
  }

 inline bool jj_3_979()
 {
    if (jj_done) return true;
    if (jj_3R_collate_clause_4076_5_153()) return true;
    return false;
  }

 inline bool jj_3_978()
 {
    if (jj_done) return true;
    if (jj_3R_column_reference_1502_5_193()) return true;
    return false;
  }

 inline bool jj_3_977()
 {
    if (jj_done) return true;
    if (jj_3R_value_expression_1855_5_178()) return true;
    return false;
  }

 inline bool jj_3R_window_partition_column_reference_3241_3_413()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_977()) {
    jj_scanpos = xsp;
    if (jj_3_978()) return true;
    }
    xsp = jj_scanpos;
    if (jj_3_979()) jj_scanpos = xsp;
    return false;
  }

 inline bool jj_3_976()
 {
    if (jj_done) return true;
    if (jj_scan_token(570)) return true;
    if (jj_3R_window_partition_column_reference_3241_3_413()) return true;
    return false;
  }

 inline bool jj_3R_window_partition_column_reference_list_3234_5_932()
 {
    if (jj_done) return true;
    if (jj_3R_window_partition_column_reference_3241_3_413()) return true;
    Token * xsp;
    while (true) {
      xsp = jj_scanpos;
      if (jj_3_976()) { jj_scanpos = xsp; break; }
    }
    return false;
  }

 inline bool jj_3R_window_partition_clause_3228_5_409()
 {
    if (jj_done) return true;
    if (jj_scan_token(PARTITION)) return true;
    if (jj_scan_token(BY)) return true;
    if (jj_3R_window_partition_column_reference_list_3234_5_932()) return true;
    return false;
  }

 inline bool jj_3R_existing_identifier_3222_5_412()
 {
    if (jj_done) return true;
    if (jj_3R_identifier_928_3_141()) return true;
    return false;
  }

 inline bool jj_3_974()
 {
    if (jj_done) return true;
    if (jj_3R_existing_identifier_3222_5_412()) return true;
    return false;
  }

 inline bool jj_3_973()
 {
    if (jj_done) return true;
    if (jj_3R_window_frame_clause_3257_5_411()) return true;
    return false;
  }

 inline bool jj_3_969()
 {
    if (jj_done) return true;
    if (jj_scan_token(570)) return true;
    if (jj_3R_window_definition_3198_5_407()) return true;
    return false;
  }

 inline bool jj_3_972()
 {
    if (jj_done) return true;
    if (jj_3R_window_order_clause_3251_5_410()) return true;
    return false;
  }

 inline bool jj_3_975()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_971()) {
    jj_scanpos = xsp;
    if (jj_3_972()) {
    jj_scanpos = xsp;
    if (jj_3_973()) {
    jj_scanpos = xsp;
    if (jj_3_974()) return true;
    }
    }
    }
    return false;
  }

 inline bool jj_3_971()
 {
    if (jj_done) return true;
    if (jj_3R_window_partition_clause_3228_5_409()) return true;
    return false;
  }

 inline bool jj_3_970()
 {
    if (jj_done) return true;
    if (jj_3R_window_specification_details_3211_3_408()) return true;
    return false;
  }

 inline bool jj_3R_window_specification_details_3211_3_408()
 {
    if (jj_done) return true;
    Token * xsp;
    if (jj_3_975()) return true;
    while (true) {
      xsp = jj_scanpos;
      if (jj_3_975()) { jj_scanpos = xsp; break; }
    }
    return false;
  }

 inline bool jj_3R_window_specification_3204_6_898()
 {
    if (jj_done) return true;
    if (jj_scan_token(lparen)) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_970()) jj_scanpos = xsp;
    if (jj_scan_token(rparen)) return true;
    return false;
  }

 inline bool jj_3R_window_definition_3198_5_407()
 {
    if (jj_done) return true;
    if (jj_3R_identifier_928_3_141()) return true;
    if (jj_scan_token(AS)) return true;
    if (jj_3R_window_specification_3204_6_898()) return true;
    return false;
  }

 inline bool jj_3R_window_definition_list_3192_5_921()
 {
    if (jj_done) return true;
    if (jj_3R_window_definition_3198_5_407()) return true;
    Token * xsp;
    while (true) {
      xsp = jj_scanpos;
      if (jj_3_969()) { jj_scanpos = xsp; break; }
    }
    return false;
  }

 inline bool jj_3R_window_clause_3186_5_368()
 {
    if (jj_done) return true;
    if (jj_scan_token(WINDOW)) return true;
    if (jj_3R_window_definition_list_3192_5_921()) return true;
    return false;
  }

 inline bool jj_3R_having_clause_3180_5_367()
 {
    if (jj_done) return true;
    if (jj_scan_token(HAVING)) return true;
    if (jj_3R_search_condition_3868_5_818()) return true;
    return false;
  }

 inline bool jj_3R_empty_grouping_set_3174_6_401()
 {
    if (jj_done) return true;
    if (jj_scan_token(lparen)) return true;
    if (jj_scan_token(rparen)) return true;
    return false;
  }

 inline bool jj_3_963()
 {
    if (jj_done) return true;
    if (jj_scan_token(570)) return true;
    if (jj_3R_grouping_set_3164_5_406()) return true;
    return false;
  }

 inline bool jj_3_968()
 {
    if (jj_done) return true;
    if (jj_3R_ordinary_grouping_set_3110_5_402()) return true;
    return false;
  }

 inline bool jj_3_967()
 {
    if (jj_done) return true;
    if (jj_3R_empty_grouping_set_3174_6_401()) return true;
    return false;
  }

 inline bool jj_3_966()
 {
    if (jj_done) return true;
    if (jj_3R_grouping_sets_specification_3152_5_400()) return true;
    return false;
  }

 inline bool jj_3_962()
 {
    if (jj_done) return true;
    if (jj_scan_token(570)) return true;
    if (jj_3R_ordinary_grouping_set_3110_5_402()) return true;
    return false;
  }

 inline bool jj_3_965()
 {
    if (jj_done) return true;
    if (jj_3R_cube_list_3146_5_399()) return true;
    return false;
  }

 inline bool jj_3R_grouping_set_3164_5_406()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_964()) {
    jj_scanpos = xsp;
    if (jj_3_965()) {
    jj_scanpos = xsp;
    if (jj_3_966()) {
    jj_scanpos = xsp;
    if (jj_3_967()) {
    jj_scanpos = xsp;
    if (jj_3_968()) return true;
    }
    }
    }
    }
    return false;
  }

 inline bool jj_3_964()
 {
    if (jj_done) return true;
    if (jj_3R_rollup_list_3134_5_398()) return true;
    return false;
  }

 inline bool jj_3R_grouping_set_list_3158_5_1052()
 {
    if (jj_done) return true;
    if (jj_3R_grouping_set_3164_5_406()) return true;
    Token * xsp;
    while (true) {
      xsp = jj_scanpos;
      if (jj_3_963()) { jj_scanpos = xsp; break; }
    }
    return false;
  }

 inline bool jj_3_961()
 {
    if (jj_done) return true;
    if (jj_scan_token(570)) return true;
    if (jj_3R_grouping_column_reference_3117_3_403()) return true;
    return false;
  }

 inline bool jj_3R_grouping_sets_specification_3152_5_400()
 {
    if (jj_done) return true;
    if (jj_scan_token(GROUPING)) return true;
    if (jj_scan_token(SETS)) return true;
    if (jj_scan_token(lparen)) return true;
    if (jj_3R_grouping_set_list_3158_5_1052()) return true;
    if (jj_scan_token(rparen)) return true;
    return false;
  }

 inline bool jj_3R_cube_list_3146_5_399()
 {
    if (jj_done) return true;
    if (jj_scan_token(CUBE)) return true;
    if (jj_scan_token(lparen)) return true;
    if (jj_3R_ordinary_grouping_set_list_3140_5_931()) return true;
    if (jj_scan_token(rparen)) return true;
    return false;
  }

 inline bool jj_3R_ordinary_grouping_set_list_3140_5_931()
 {
    if (jj_done) return true;
    if (jj_3R_ordinary_grouping_set_3110_5_402()) return true;
    Token * xsp;
    while (true) {
      xsp = jj_scanpos;
      if (jj_3_962()) { jj_scanpos = xsp; break; }
    }
    return false;
  }

 inline bool jj_3R_rollup_list_3134_5_398()
 {
    if (jj_done) return true;
    if (jj_scan_token(ROLLUP)) return true;
    if (jj_scan_token(lparen)) return true;
    if (jj_3R_ordinary_grouping_set_list_3140_5_931()) return true;
    if (jj_scan_token(rparen)) return true;
    return false;
  }

 inline bool jj_3R_grouping_column_reference_list_3128_5_404()
 {
    if (jj_done) return true;
    if (jj_3R_grouping_column_reference_3117_3_403()) return true;
    Token * xsp;
    while (true) {
      xsp = jj_scanpos;
      if (jj_3_961()) { jj_scanpos = xsp; break; }
    }
    return false;
  }

 inline bool jj_3_960()
 {
    if (jj_done) return true;
    if (jj_3R_collate_clause_4076_5_153()) return true;
    return false;
  }

 inline bool jj_3_959()
 {
    if (jj_done) return true;
    if (jj_3R_column_reference_1502_5_193()) return true;
    return false;
  }

 inline bool jj_3_958()
 {
    if (jj_done) return true;
    if (jj_3R_grouping_expression_7990_5_405()) return true;
    return false;
  }

 inline bool jj_3R_grouping_column_reference_3117_3_403()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_958()) {
    jj_scanpos = xsp;
    if (jj_3_959()) return true;
    }
    xsp = jj_scanpos;
    if (jj_3_960()) jj_scanpos = xsp;
    return false;
  }

 inline bool jj_3_950()
 {
    if (jj_done) return true;
    if (jj_scan_token(570)) return true;
    if (jj_3R_grouping_element_3100_5_397()) return true;
    return false;
  }

 inline bool jj_3_957()
 {
    if (jj_done) return true;
    if (jj_scan_token(lparen)) return true;
    if (jj_3R_grouping_column_reference_list_3128_5_404()) return true;
    if (jj_scan_token(rparen)) return true;
    return false;
  }

 inline bool jj_3R_ordinary_grouping_set_3110_5_402()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_956()) {
    jj_scanpos = xsp;
    if (jj_3_957()) return true;
    }
    return false;
  }

 inline bool jj_3_956()
 {
    if (jj_done) return true;
    if (jj_3R_grouping_column_reference_3117_3_403()) return true;
    return false;
  }

 inline bool jj_3_955()
 {
    if (jj_done) return true;
    if (jj_3R_ordinary_grouping_set_3110_5_402()) return true;
    return false;
  }

 inline bool jj_3_954()
 {
    if (jj_done) return true;
    if (jj_3R_empty_grouping_set_3174_6_401()) return true;
    return false;
  }

 inline bool jj_3_953()
 {
    if (jj_done) return true;
    if (jj_3R_grouping_sets_specification_3152_5_400()) return true;
    return false;
  }

 inline bool jj_3_952()
 {
    if (jj_done) return true;
    if (jj_3R_cube_list_3146_5_399()) return true;
    return false;
  }

 inline bool jj_3R_grouping_element_3100_5_397()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_951()) {
    jj_scanpos = xsp;
    if (jj_3_952()) {
    jj_scanpos = xsp;
    if (jj_3_953()) {
    jj_scanpos = xsp;
    if (jj_3_954()) {
    jj_scanpos = xsp;
    if (jj_3_955()) return true;
    }
    }
    }
    }
    return false;
  }

 inline bool jj_3_951()
 {
    if (jj_done) return true;
    if (jj_3R_rollup_list_3134_5_398()) return true;
    return false;
  }

 inline bool jj_3R_grouping_element_list_3094_5_920()
 {
    if (jj_done) return true;
    if (jj_3R_grouping_element_3100_5_397()) return true;
    Token * xsp;
    while (true) {
      xsp = jj_scanpos;
      if (jj_3_950()) { jj_scanpos = xsp; break; }
    }
    return false;
  }

 inline bool jj_3_949()
 {
    if (jj_done) return true;
    if (jj_3R_set_quantifier_4159_5_396()) return true;
    return false;
  }

 inline bool jj_3R_group_by_clause_3086_5_366()
 {
    if (jj_done) return true;
    if (jj_scan_token(GROUP)) return true;
    if (jj_scan_token(BY)) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_949()) jj_scanpos = xsp;
    if (jj_3R_grouping_element_list_3094_5_920()) return true;
    return false;
  }

 inline bool jj_3R_where_clause_3080_5_365()
 {
    if (jj_done) return true;
    if (jj_scan_token(WHERE)) return true;
    if (jj_3R_search_condition_3868_5_818()) return true;
    return false;
  }

 inline bool jj_3_943()
 {
    if (jj_done) return true;
    if (jj_scan_token(OUTER)) return true;
    return false;
  }

 inline bool jj_3R_join_column_list_3074_5_930()
 {
    if (jj_done) return true;
    if (jj_3R_column_name_list_2942_5_191()) return true;
    return false;
  }

 inline bool jj_3_948()
 {
    if (jj_done) return true;
    if (jj_scan_token(FULL)) return true;
    return false;
  }

 inline bool jj_3_947()
 {
    if (jj_done) return true;
    if (jj_scan_token(RIGHT)) return true;
    return false;
  }

 inline bool jj_3R_outer_join_type_3066_5_395()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_946()) {
    jj_scanpos = xsp;
    if (jj_3_947()) {
    jj_scanpos = xsp;
    if (jj_3_948()) return true;
    }
    }
    return false;
  }

 inline bool jj_3_946()
 {
    if (jj_done) return true;
    if (jj_scan_token(LEFT)) return true;
    return false;
  }

 inline bool jj_3_945()
 {
    if (jj_done) return true;
    if (jj_3R_outer_join_type_3066_5_395()) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_943()) jj_scanpos = xsp;
    return false;
  }

 inline bool jj_3R_join_type_3059_5_390()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_944()) {
    jj_scanpos = xsp;
    if (jj_3_945()) return true;
    }
    return false;
  }

 inline bool jj_3_944()
 {
    if (jj_done) return true;
    if (jj_scan_token(INNER)) return true;
    return false;
  }

 inline bool jj_3R_named_columns_join_3053_5_394()
 {
    if (jj_done) return true;
    if (jj_scan_token(USING)) return true;
    if (jj_scan_token(lparen)) return true;
    if (jj_3R_join_column_list_3074_5_930()) return true;
    if (jj_scan_token(rparen)) return true;
    return false;
  }

 inline bool jj_3_940()
 {
    if (jj_done) return true;
    if (jj_3R_partitioned_join_table_3012_5_372()) return true;
    return false;
  }

 inline bool jj_3R_join_condition_3047_5_393()
 {
    if (jj_done) return true;
    if (jj_scan_token(ON)) return true;
    if (jj_3R_search_condition_3868_5_818()) return true;
    return false;
  }

 inline bool jj_3_938()
 {
    if (jj_done) return true;
    if (jj_3R_join_type_3059_5_390()) return true;
    return false;
  }

 inline bool jj_3_942()
 {
    if (jj_done) return true;
    if (jj_3R_named_columns_join_3053_5_394()) return true;
    return false;
  }

 inline bool jj_3R_join_specification_3040_5_928()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_941()) {
    jj_scanpos = xsp;
    if (jj_3_942()) return true;
    }
    return false;
  }

 inline bool jj_3_941()
 {
    if (jj_done) return true;
    if (jj_3R_join_condition_3047_5_393()) return true;
    return false;
  }

 inline bool jj_3_939()
 {
    if (jj_done) return true;
    if (jj_3R_table_factor_2825_5_392()) return true;
    return false;
  }

 inline bool jj_3R_natural_join_3032_5_389()
 {
    if (jj_done) return true;
    if (jj_scan_token(NATURAL)) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_938()) jj_scanpos = xsp;
    if (jj_scan_token(JOIN)) return true;
    xsp = jj_scanpos;
    if (jj_3_939()) {
    jj_scanpos = xsp;
    if (jj_3_940()) return true;
    }
    return false;
  }

 inline bool jj_3_936()
 {
    if (jj_done) return true;
    if (jj_3R_partitioned_join_table_3012_5_372()) return true;
    return false;
  }

 inline bool jj_3R_partitioned_join_column_reference_3026_5_391()
 {
    if (jj_done) return true;
    if (jj_3R_column_reference_1502_5_193()) return true;
    return false;
  }

 inline bool jj_3_937()
 {
    if (jj_done) return true;
    if (jj_scan_token(570)) return true;
    if (jj_3R_partitioned_join_column_reference_3026_5_391()) return true;
    return false;
  }

 inline bool jj_3R_partitioned_join_column_reference_list_3018_6_923()
 {
    if (jj_done) return true;
    if (jj_scan_token(lparen)) return true;
    if (jj_3R_partitioned_join_column_reference_3026_5_391()) return true;
    Token * xsp;
    while (true) {
      xsp = jj_scanpos;
      if (jj_3_937()) { jj_scanpos = xsp; break; }
    }
    if (jj_scan_token(rparen)) return true;
    return false;
  }

 inline bool jj_3R_partitioned_join_table_3012_5_372()
 {
    if (jj_done) return true;
    if (jj_scan_token(PARTITION)) return true;
    if (jj_scan_token(BY)) return true;
    if (jj_3R_partitioned_join_column_reference_list_3018_6_923()) return true;
    return false;
  }

 inline bool jj_3_935()
 {
    if (jj_done) return true;
    if (jj_3R_table_reference_2819_5_369()) return true;
    return false;
  }

 inline bool jj_3_934()
 {
    if (jj_done) return true;
    if (jj_3R_join_type_3059_5_390()) return true;
    return false;
  }

 inline bool jj_3R_qualified_join_3004_5_388()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_934()) jj_scanpos = xsp;
    if (jj_scan_token(JOIN)) return true;
    xsp = jj_scanpos;
    if (jj_3_935()) {
    jj_scanpos = xsp;
    if (jj_3_936()) return true;
    }
    if (jj_3R_join_specification_3040_5_928()) return true;
    return false;
  }

 inline bool jj_3R_cross_join_2997_6_387()
 {
    if (jj_done) return true;
    if (jj_scan_token(CROSS)) return true;
    if (jj_scan_token(JOIN)) return true;
    if (jj_3R_table_factor_2825_5_392()) return true;
    return false;
  }

 inline bool jj_3_928()
 {
    if (jj_done) return true;
    if (jj_scan_token(lparen)) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_scan_token(554)) {
    jj_scanpos = xsp;
    if (jj_scan_token(508)) {
    jj_scanpos = xsp;
    if (jj_scan_token(328)) {
    jj_scanpos = xsp;
    if (jj_scan_token(527)) return true;
    }
    }
    }
    return false;
  }

 inline bool jj_3_933()
 {
    if (jj_done) return true;
    if (jj_3R_natural_join_3032_5_389()) return true;
    return false;
  }

 inline bool jj_3_932()
 {
    if (jj_done) return true;
    if (jj_3R_qualified_join_3004_5_388()) return true;
    return false;
  }

 inline bool jj_3R_joined_table_2988_5_370()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_931()) {
    jj_scanpos = xsp;
    if (jj_3_932()) {
    jj_scanpos = xsp;
    if (jj_3_933()) return true;
    }
    }
    return false;
  }

 inline bool jj_3_931()
 {
    if (jj_done) return true;
    if (jj_3R_cross_join_2997_6_387()) return true;
    return false;
  }

 inline bool jj_3_930()
 {
    if (jj_done) return true;
    if (jj_3R_joined_table_2988_5_370()) return true;
    return false;
  }

 inline bool jj_3_929()
 {
    if (jj_done) return true;
    if (jj_3R_table_reference_2819_5_369()) return true;
    return false;
  }

 inline bool jj_3R_parenthesized_joined_table_2977_10_925()
 {
    if (jj_done) return true;
    if (jj_3R_table_primary_2865_3_929()) return true;
    return false;
  }

 inline bool jj_3R_parenthesized_joined_table_2971_5_375()
 {
    if (jj_done) return true;
    if (jj_scan_token(lparen)) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3R_parenthesized_joined_table_2977_10_925()) {
    jj_scanpos = xsp;
    if (jj_3_929()) return true;
    }
    while (true) {
      xsp = jj_scanpos;
      if (jj_3_930()) { jj_scanpos = xsp; break; }
    }
    if (jj_scan_token(rparen)) return true;
    return false;
  }

 inline bool jj_3_927()
 {
    if (jj_done) return true;
    if (jj_scan_token(OLD)) return true;
    return false;
  }

 inline bool jj_3_926()
 {
    if (jj_done) return true;
    if (jj_scan_token(NEW)) return true;
    return false;
  }

 inline bool jj_3R_result_option_2963_5_926()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_925()) {
    jj_scanpos = xsp;
    if (jj_3_926()) {
    jj_scanpos = xsp;
    if (jj_3_927()) return true;
    }
    }
    return false;
  }

 inline bool jj_3_925()
 {
    if (jj_done) return true;
    if (jj_scan_token(FINAL)) return true;
    return false;
  }

 inline bool jj_3_924()
 {
    if (jj_done) return true;
    if (jj_3R_update_statement_searched_6963_5_386()) return true;
    return false;
  }

 inline bool jj_3_920()
 {
    if (jj_done) return true;
    if (jj_scan_token(570)) return true;
    if (jj_3R_identifier_928_3_141()) return true;
    return false;
  }

 inline bool jj_3_923()
 {
    if (jj_done) return true;
    if (jj_3R_merge_statement_6878_5_385()) return true;
    return false;
  }

 inline bool jj_3_922()
 {
    if (jj_done) return true;
    if (jj_3R_insert_statement_6823_5_384()) return true;
    return false;
  }

 inline bool jj_3R_data_change_statement_2954_5_1053()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_921()) {
    jj_scanpos = xsp;
    if (jj_3_922()) {
    jj_scanpos = xsp;
    if (jj_3_923()) {
    jj_scanpos = xsp;
    if (jj_3_924()) return true;
    }
    }
    }
    return false;
  }

 inline bool jj_3_921()
 {
    if (jj_done) return true;
    if (jj_3R_delete_statement_searched_6803_5_383()) return true;
    return false;
  }

 inline bool jj_3R_data_change_delta_table_2948_5_380()
 {
    if (jj_done) return true;
    if (jj_3R_result_option_2963_5_926()) return true;
    if (jj_scan_token(TABLE)) return true;
    if (jj_scan_token(lparen)) return true;
    if (jj_3R_data_change_statement_2954_5_1053()) return true;
    if (jj_scan_token(rparen)) return true;
    return false;
  }

 inline bool jj_3R_column_name_list_2942_5_191()
 {
    if (jj_done) return true;
    if (jj_3R_identifier_928_3_141()) return true;
    Token * xsp;
    while (true) {
      xsp = jj_scanpos;
      if (jj_3_920()) { jj_scanpos = xsp; break; }
    }
    return false;
  }

 inline bool jj_3_916()
 {
    if (jj_done) return true;
    if (jj_scan_token(lparen)) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_scan_token(508)) {
    jj_scanpos = xsp;
    if (jj_scan_token(328)) {
    jj_scanpos = xsp;
    if (jj_scan_token(527)) return true;
    }
    }
    return false;
  }

 inline bool jj_3_919()
 {
    if (jj_done) return true;
    if (jj_3R_identifier_928_3_141()) return true;
    return false;
  }

 inline bool jj_3R_table_or_query_name_2935_5_374()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_918()) {
    jj_scanpos = xsp;
    if (jj_3_919()) return true;
    }
    return false;
  }

 inline bool jj_3_918()
 {
    if (jj_done) return true;
    if (jj_3R_table_name_948_5_382()) return true;
    return false;
  }

 inline bool jj_3_908()
 {
    if (jj_done) return true;
    if (jj_scan_token(SYMMETRIC)) return true;
    return false;
  }

 inline bool jj_3_917()
 {
    if (jj_done) return true;
    if (jj_3R_subquery_3527_5_181()) return true;
    return false;
  }

 inline bool jj_3R_derived_table_2928_5_1040()
 {
    if (jj_done) return true;
    if (jj_3R_query_expression_3399_5_547()) return true;
    return false;
  }

 inline bool jj_3R_derived_table_2928_5_1037()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3R_derived_table_2928_5_1040()) {
    jj_scanpos = xsp;
    if (jj_3_917()) return true;
    }
    return false;
  }

 inline bool jj_3R_table_function_derived_table_2922_5_378()
 {
    if (jj_done) return true;
    if (jj_scan_token(TABLE)) return true;
    if (jj_scan_token(lparen)) return true;
    if (jj_3R_collection_value_expression_1887_5_267()) return true;
    if (jj_scan_token(rparen)) return true;
    return false;
  }

 inline bool jj_3_915()
 {
    if (jj_done) return true;
    if (jj_scan_token(WITH)) return true;
    if (jj_scan_token(ORDINALITY)) return true;
    return false;
  }

 inline bool jj_3_914()
 {
    if (jj_done) return true;
    if (jj_scan_token(570)) return true;
    if (jj_3R_collection_value_expression_1887_5_267()) return true;
    return false;
  }

 inline bool jj_3_909()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_907()) {
    jj_scanpos = xsp;
    if (jj_3_908()) return true;
    }
    return false;
  }

 inline bool jj_3_907()
 {
    if (jj_done) return true;
    if (jj_scan_token(ASYMMETRIC)) return true;
    return false;
  }

 inline bool jj_3R_collection_derived_table_2914_5_377()
 {
    if (jj_done) return true;
    if (jj_scan_token(UNNEST)) return true;
    if (jj_scan_token(lparen)) return true;
    if (jj_3R_collection_value_expression_1887_5_267()) return true;
    Token * xsp;
    while (true) {
      xsp = jj_scanpos;
      if (jj_3_914()) { jj_scanpos = xsp; break; }
    }
    if (jj_scan_token(rparen)) return true;
    xsp = jj_scanpos;
    if (jj_3_915()) jj_scanpos = xsp;
    return false;
  }

 inline bool jj_3R_lateral_derived_table_2908_5_376()
 {
    if (jj_done) return true;
    if (jj_scan_token(LATERAL)) return true;
    if (jj_3R_subquery_3527_5_181()) return true;
    return false;
  }

 inline bool jj_3R_only_spec_2901_5_379()
 {
    if (jj_done) return true;
    if (jj_scan_token(ONLY)) return true;
    if (jj_scan_token(lparen)) return true;
    if (jj_3R_table_or_query_name_2935_5_374()) return true;
    if (jj_scan_token(rparen)) return true;
    return false;
  }

 inline bool jj_3_913()
 {
    if (jj_done) return true;
    if (jj_scan_token(VERSIONS)) return true;
    if (jj_scan_token(BETWEEN)) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_909()) jj_scanpos = xsp;
    if (jj_scan_token(SYSTEM)) return true;
    return false;
  }

 inline bool jj_3_912()
 {
    if (jj_done) return true;
    if (jj_scan_token(VERSIONS)) return true;
    if (jj_scan_token(AFTER)) return true;
    if (jj_scan_token(SYSTEM)) return true;
    return false;
  }

 inline bool jj_3_911()
 {
    if (jj_done) return true;
    if (jj_scan_token(VERSIONS)) return true;
    if (jj_scan_token(BEFORE)) return true;
    if (jj_scan_token(SYSTEM)) return true;
    return false;
  }

 inline bool jj_3_910()
 {
    if (jj_done) return true;
    if (jj_scan_token(AS)) return true;
    if (jj_scan_token(OF)) return true;
    if (jj_scan_token(SYSTEM)) return true;
    return false;
  }

 inline bool jj_3_906()
 {
    if (jj_done) return true;
    if (jj_scan_token(lparen)) return true;
    if (jj_3R_column_name_list_2942_5_191()) return true;
    if (jj_scan_token(rparen)) return true;
    return false;
  }

 inline bool jj_3_905()
 {
    if (jj_done) return true;
    if (jj_3R_identifier_suffix_chain_7944_5_138()) return true;
    return false;
  }

 inline bool jj_3_904()
 {
    if (jj_done) return true;
    if (jj_scan_token(AS)) return true;
    return false;
  }

 inline bool jj_3R_alias_2882_5_381()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_904()) jj_scanpos = xsp;
    if (jj_3R_identifier_928_3_141()) return true;
    xsp = jj_scanpos;
    if (jj_3_905()) jj_scanpos = xsp;
    xsp = jj_scanpos;
    if (jj_3_906()) jj_scanpos = xsp;
    return false;
  }

 inline bool jj_3_896()
 {
    if (jj_done) return true;
    if (jj_scan_token(lparen)) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_scan_token(554)) {
    jj_scanpos = xsp;
    if (jj_scan_token(508)) {
    jj_scanpos = xsp;
    if (jj_scan_token(328)) {
    jj_scanpos = xsp;
    if (jj_scan_token(527)) return true;
    }
    }
    }
    return false;
  }

 inline bool jj_3_903()
 {
    if (jj_done) return true;
    if (jj_3R_alias_2882_5_381()) return true;
    return false;
  }

 inline bool jj_3_902()
 {
    if (jj_done) return true;
    if (jj_3R_data_change_delta_table_2948_5_380()) return true;
    return false;
  }

 inline bool jj_3_901()
 {
    if (jj_done) return true;
    if (jj_3R_only_spec_2901_5_379()) return true;
    return false;
  }

 inline bool jj_3_900()
 {
    if (jj_done) return true;
    if (jj_3R_table_function_derived_table_2922_5_378()) return true;
    return false;
  }

 inline bool jj_3_899()
 {
    if (jj_done) return true;
    if (jj_3R_collection_derived_table_2914_5_377()) return true;
    return false;
  }

 inline bool jj_3_898()
 {
    if (jj_done) return true;
    if (jj_3R_lateral_derived_table_2908_5_376()) return true;
    return false;
  }

 inline bool jj_3_897()
 {
    if (jj_done) return true;
    if (jj_3R_parenthesized_joined_table_2971_5_375()) return true;
    return false;
  }

 inline bool jj_3R_table_primary_2867_5_1025()
 {
    if (jj_done) return true;
    if (jj_3R_derived_table_2928_5_1037()) return true;
    return false;
  }

 inline bool jj_3_895()
 {
    if (jj_done) return true;
    if (jj_3R_table_or_query_name_2935_5_374()) return true;
    return false;
  }

 inline bool jj_3R_table_primary_2865_3_929()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_895()) {
    jj_scanpos = xsp;
    if (jj_3R_table_primary_2867_5_1025()) {
    jj_scanpos = xsp;
    if (jj_3_897()) {
    jj_scanpos = xsp;
    if (jj_3_898()) {
    jj_scanpos = xsp;
    if (jj_3_899()) {
    jj_scanpos = xsp;
    if (jj_3_900()) {
    jj_scanpos = xsp;
    if (jj_3_901()) {
    jj_scanpos = xsp;
    if (jj_3_902()) return true;
    }
    }
    }
    }
    }
    }
    }
    xsp = jj_scanpos;
    if (jj_3_903()) jj_scanpos = xsp;
    return false;
  }

 inline bool jj_3R_repeat_argument_2859_5_924()
 {
    if (jj_done) return true;
    if (jj_3R_numeric_value_expression_1894_5_261()) return true;
    return false;
  }

 inline bool jj_3R_sample_percentage_2853_5_1051()
 {
    if (jj_done) return true;
    if (jj_3R_numeric_value_expression_1894_5_261()) return true;
    return false;
  }

 inline bool jj_3R_repeatable_clause_2847_5_373()
 {
    if (jj_done) return true;
    if (jj_scan_token(REPEATABLE)) return true;
    if (jj_scan_token(lparen)) return true;
    if (jj_3R_repeat_argument_2859_5_924()) return true;
    if (jj_scan_token(rparen)) return true;
    return false;
  }

 inline bool jj_3_890()
 {
    if (jj_done) return true;
    if (jj_3R_sample_clause_2833_5_371()) return true;
    return false;
  }

 inline bool jj_3_894()
 {
    if (jj_done) return true;
    if (jj_scan_token(SYSTEM)) return true;
    return false;
  }

 inline bool jj_3R_sample_method_2840_5_922()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_893()) {
    jj_scanpos = xsp;
    if (jj_3_894()) return true;
    }
    return false;
  }

 inline bool jj_3_893()
 {
    if (jj_done) return true;
    if (jj_scan_token(BERNOULLI)) return true;
    return false;
  }

 inline bool jj_3_892()
 {
    if (jj_done) return true;
    if (jj_3R_repeatable_clause_2847_5_373()) return true;
    return false;
  }

 inline bool jj_3_889()
 {
    if (jj_done) return true;
    if (jj_3R_joined_table_2988_5_370()) return true;
    return false;
  }

 inline bool jj_3R_sample_clause_2833_5_371()
 {
    if (jj_done) return true;
    if (jj_scan_token(TABLESAMPLE)) return true;
    if (jj_3R_sample_method_2840_5_922()) return true;
    if (jj_scan_token(lparen)) return true;
    if (jj_3R_sample_percentage_2853_5_1051()) return true;
    if (jj_scan_token(rparen)) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_892()) jj_scanpos = xsp;
    return false;
  }

 inline bool jj_3_888()
 {
    if (jj_done) return true;
    if (jj_scan_token(570)) return true;
    if (jj_3R_table_reference_2819_5_369()) return true;
    return false;
  }

 inline bool jj_3_891()
 {
    if (jj_done) return true;
    if (jj_3R_partitioned_join_table_3012_5_372()) return true;
    return false;
  }

 inline bool jj_3R_table_factor_2825_5_392()
 {
    if (jj_done) return true;
    if (jj_3R_table_primary_2865_3_929()) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_890()) jj_scanpos = xsp;
    xsp = jj_scanpos;
    if (jj_3_891()) jj_scanpos = xsp;
    return false;
  }

 inline bool jj_3R_table_reference_2819_5_369()
 {
    if (jj_done) return true;
    if (jj_3R_table_factor_2825_5_392()) return true;
    Token * xsp;
    while (true) {
      xsp = jj_scanpos;
      if (jj_3_889()) { jj_scanpos = xsp; break; }
    }
    return false;
  }

 inline bool jj_3R_table_reference_list_2813_5_1026()
 {
    if (jj_done) return true;
    if (jj_3R_table_reference_2819_5_369()) return true;
    Token * xsp;
    while (true) {
      xsp = jj_scanpos;
      if (jj_3_888()) { jj_scanpos = xsp; break; }
    }
    return false;
  }

 inline bool jj_3_882()
 {
    if (jj_done) return true;
    if (jj_scan_token(570)) return true;
    if (jj_3R_table_row_value_expression_2744_5_363()) return true;
    return false;
  }

 inline bool jj_3R_from_clause_2807_5_936()
 {
    if (jj_done) return true;
    if (jj_scan_token(FROM)) return true;
    if (jj_3R_table_reference_list_2813_5_1026()) return true;
    return false;
  }

 inline bool jj_3_887()
 {
    if (jj_done) return true;
    if (jj_3R_window_clause_3186_5_368()) return true;
    return false;
  }

 inline bool jj_3_886()
 {
    if (jj_done) return true;
    if (jj_3R_having_clause_3180_5_367()) return true;
    return false;
  }

 inline bool jj_3_885()
 {
    if (jj_done) return true;
    if (jj_3R_group_by_clause_3086_5_366()) return true;
    return false;
  }

 inline bool jj_3_884()
 {
    if (jj_done) return true;
    if (jj_3R_where_clause_3080_5_365()) return true;
    return false;
  }

 inline bool jj_3R_table_expression_2797_5_419()
 {
    if (jj_done) return true;
    if (jj_3R_from_clause_2807_5_936()) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_884()) jj_scanpos = xsp;
    xsp = jj_scanpos;
    if (jj_3_885()) jj_scanpos = xsp;
    xsp = jj_scanpos;
    if (jj_3_886()) jj_scanpos = xsp;
    xsp = jj_scanpos;
    if (jj_3_887()) jj_scanpos = xsp;
    return false;
  }

 inline bool jj_3_883()
 {
    if (jj_done) return true;
    if (jj_scan_token(570)) return true;
    if (jj_3R_contextually_typed_row_value_expression_2751_5_364()) return true;
    return false;
  }

 inline bool jj_3R_contextually_typed_row_value_expression_list_2790_5_1035()
 {
    if (jj_done) return true;
    if (jj_3R_contextually_typed_row_value_expression_2751_5_364()) return true;
    Token * xsp;
    while (true) {
      xsp = jj_scanpos;
      if (jj_3_883()) { jj_scanpos = xsp; break; }
    }
    return false;
  }

 inline bool jj_3R_contextually_typed_table_value_constructor_2784_5_1009()
 {
    if (jj_done) return true;
    if (jj_scan_token(VALUES)) return true;
    if (jj_3R_contextually_typed_row_value_expression_list_2790_5_1035()) return true;
    return false;
  }

 inline bool jj_3R_row_value_expression_list_2778_5_939()
 {
    if (jj_done) return true;
    if (jj_3R_table_row_value_expression_2744_5_363()) return true;
    Token * xsp;
    while (true) {
      xsp = jj_scanpos;
      if (jj_3_882()) { jj_scanpos = xsp; break; }
    }
    return false;
  }

 inline bool jj_3R_table_value_constructor_2772_5_438()
 {
    if (jj_done) return true;
    if (jj_scan_token(VALUES)) return true;
    if (jj_3R_row_value_expression_list_2778_5_939()) return true;
    return false;
  }

 inline bool jj_3_881()
 {
    if (jj_done) return true;
    if (jj_3R_nonparenthesized_value_expression_primary_1333_5_177()) return true;
    return false;
  }

 inline bool jj_3R_row_value_special_case_2765_5_359()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_880()) {
    jj_scanpos = xsp;
    if (jj_3_881()) return true;
    }
    return false;
  }

 inline bool jj_3_880()
 {
    if (jj_done) return true;
    if (jj_3R_common_value_expression_1863_5_259()) return true;
    return false;
  }

 inline bool jj_3_879()
 {
    if (jj_done) return true;
    if (jj_3R_row_value_special_case_2765_5_359()) return true;
    return false;
  }

 inline bool jj_3_878()
 {
    if (jj_done) return true;
    if (jj_3R_row_value_constructor_predicand_2729_5_362()) return true;
    return false;
  }

 inline bool jj_3R_row_value_predicand_2758_5_229()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_878()) {
    jj_scanpos = xsp;
    if (jj_3_879()) return true;
    }
    return false;
  }

 inline bool jj_3_877()
 {
    if (jj_done) return true;
    if (jj_3R_row_value_special_case_2765_5_359()) return true;
    return false;
  }

 inline bool jj_3R_contextually_typed_row_value_expression_2751_5_364()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_876()) {
    jj_scanpos = xsp;
    if (jj_3_877()) return true;
    }
    return false;
  }

 inline bool jj_3_876()
 {
    if (jj_done) return true;
    if (jj_3R_contextually_typed_row_value_constructor_2703_5_361()) return true;
    return false;
  }

 inline bool jj_3_875()
 {
    if (jj_done) return true;
    if (jj_3R_row_value_special_case_2765_5_359()) return true;
    return false;
  }

 inline bool jj_3R_table_row_value_expression_2744_5_363()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_874()) {
    jj_scanpos = xsp;
    if (jj_3_875()) return true;
    }
    return false;
  }

 inline bool jj_3_874()
 {
    if (jj_done) return true;
    if (jj_3R_row_value_constructor_2675_5_360()) return true;
    return false;
  }

 inline bool jj_3_873()
 {
    if (jj_done) return true;
    if (jj_3R_row_value_special_case_2765_5_359()) return true;
    return false;
  }

 inline bool jj_3R_row_value_expression_2737_5_260()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_872()) {
    jj_scanpos = xsp;
    if (jj_3_873()) return true;
    }
    return false;
  }

 inline bool jj_3_872()
 {
    if (jj_done) return true;
    if (jj_3R_explicit_row_value_constructor_2683_5_354()) return true;
    return false;
  }

 inline bool jj_3_871()
 {
    if (jj_done) return true;
    if (jj_3R_explicit_row_value_constructor_2683_5_354()) return true;
    return false;
  }

 inline bool jj_3R_row_value_constructor_predicand_2729_5_362()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_870()) {
    jj_scanpos = xsp;
    if (jj_3_871()) return true;
    }
    return false;
  }

 inline bool jj_3_870()
 {
    if (jj_done) return true;
    if (jj_3R_common_value_expression_1863_5_259()) return true;
    return false;
  }

 inline bool jj_3_862()
 {
    if (jj_done) return true;
    if (jj_scan_token(570)) return true;
    if (jj_3R_row_value_constructor_element_2697_5_356()) return true;
    return false;
  }

 inline bool jj_3_858()
 {
    if (jj_done) return true;
    if (jj_scan_token(570)) return true;
    if (jj_3R_row_value_constructor_element_list_2691_5_355()) return true;
    return false;
  }

 inline bool jj_3R_contextually_typed_row_value_constructor_element_2722_5_357()
 {
    if (jj_done) return true;
    if (jj_3R_contextually_typed_value_specification_1475_5_194()) return true;
    return false;
  }

 inline bool jj_3_869()
 {
    if (jj_done) return true;
    if (jj_scan_token(570)) return true;
    if (jj_3R_contextually_typed_row_value_constructor_element_2722_5_357()) return true;
    return false;
  }

 inline bool jj_3R_contextually_typed_row_value_constructor_element_list_2715_5_358()
 {
    if (jj_done) return true;
    if (jj_3R_contextually_typed_row_value_constructor_element_2722_5_357()) return true;
    Token * xsp;
    while (true) {
      xsp = jj_scanpos;
      if (jj_3_869()) { jj_scanpos = xsp; break; }
    }
    return false;
  }

 inline bool jj_3_868()
 {
    if (jj_done) return true;
    if (jj_scan_token(ROW)) return true;
    if (jj_scan_token(lparen)) return true;
    if (jj_3R_contextually_typed_row_value_constructor_element_list_2715_5_358()) return true;
    if (jj_scan_token(rparen)) return true;
    return false;
  }

 inline bool jj_3_867()
 {
    if (jj_done) return true;
    if (jj_scan_token(lparen)) return true;
    if (jj_3R_contextually_typed_row_value_constructor_element_2722_5_357()) return true;
    if (jj_scan_token(570)) return true;
    if (jj_3R_contextually_typed_row_value_constructor_element_list_2715_5_358()) return true;
    if (jj_scan_token(rparen)) return true;
    return false;
  }

 inline bool jj_3_866()
 {
    if (jj_done) return true;
    if (jj_scan_token(lparen)) return true;
    if (jj_3R_contextually_typed_value_specification_1475_5_194()) return true;
    if (jj_scan_token(rparen)) return true;
    return false;
  }

 inline bool jj_3_840()
 {
    if (jj_done) return true;
    if (jj_scan_token(DISTINCT)) return true;
    return false;
  }

 inline bool jj_3_865()
 {
    if (jj_done) return true;
    if (jj_3R_contextually_typed_value_specification_1475_5_194()) return true;
    return false;
  }

 inline bool jj_3_864()
 {
    if (jj_done) return true;
    if (jj_3R_boolean_value_expression_2494_5_258()) return true;
    return false;
  }

 inline bool jj_3R_contextually_typed_row_value_constructor_2703_5_361()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_863()) {
    jj_scanpos = xsp;
    if (jj_3_864()) {
    jj_scanpos = xsp;
    if (jj_3_865()) {
    jj_scanpos = xsp;
    if (jj_3_866()) {
    jj_scanpos = xsp;
    if (jj_3_867()) {
    jj_scanpos = xsp;
    if (jj_3_868()) return true;
    }
    }
    }
    }
    }
    return false;
  }

 inline bool jj_3_863()
 {
    if (jj_done) return true;
    if (jj_3R_common_value_expression_1863_5_259()) return true;
    return false;
  }

 inline bool jj_3_841()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_839()) {
    jj_scanpos = xsp;
    if (jj_3_840()) return true;
    }
    return false;
  }

 inline bool jj_3_839()
 {
    if (jj_done) return true;
    if (jj_scan_token(ALL)) return true;
    return false;
  }

 inline bool jj_3R_row_value_constructor_element_2697_5_356()
 {
    if (jj_done) return true;
    if (jj_3R_value_expression_1855_5_178()) return true;
    return false;
  }

 inline bool jj_3R_row_value_constructor_element_list_2691_5_355()
 {
    if (jj_done) return true;
    if (jj_3R_row_value_constructor_element_2697_5_356()) return true;
    Token * xsp;
    while (true) {
      xsp = jj_scanpos;
      if (jj_3_862()) { jj_scanpos = xsp; break; }
    }
    return false;
  }

 inline bool jj_3_861()
 {
    if (jj_done) return true;
    if (jj_scan_token(lparen)) return true;
    if (jj_3R_row_value_constructor_element_2697_5_356()) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_858()) jj_scanpos = xsp;
    if (jj_scan_token(rparen)) return true;
    return false;
  }

 inline bool jj_3_860()
 {
    if (jj_done) return true;
    if (jj_3R_subquery_3527_5_181()) return true;
    return false;
  }

 inline bool jj_3R_explicit_row_value_constructor_2683_5_354()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_859()) {
    jj_scanpos = xsp;
    if (jj_3_860()) {
    jj_scanpos = xsp;
    if (jj_3_861()) return true;
    }
    }
    return false;
  }

 inline bool jj_3_859()
 {
    if (jj_done) return true;
    if (jj_scan_token(ROW)) return true;
    if (jj_scan_token(lparen)) return true;
    if (jj_3R_row_value_constructor_element_list_2691_5_355()) return true;
    if (jj_scan_token(rparen)) return true;
    return false;
  }

 inline bool jj_3_857()
 {
    if (jj_done) return true;
    if (jj_3R_boolean_value_expression_2494_5_258()) return true;
    return false;
  }

 inline bool jj_3_856()
 {
    if (jj_done) return true;
    if (jj_3R_common_value_expression_1863_5_259()) return true;
    return false;
  }

 inline bool jj_3_843()
 {
    if (jj_done) return true;
    if (jj_scan_token(MULTISET)) return true;
    if (jj_scan_token(EXCEPT)) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_841()) jj_scanpos = xsp;
    return false;
  }

 inline bool jj_3R_row_value_constructor_2675_5_360()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_855()) {
    jj_scanpos = xsp;
    if (jj_3_856()) {
    jj_scanpos = xsp;
    if (jj_3_857()) return true;
    }
    }
    return false;
  }

 inline bool jj_3_855()
 {
    if (jj_done) return true;
    if (jj_3R_explicit_row_value_constructor_2683_5_354()) return true;
    return false;
  }

 inline bool jj_3_854()
 {
    if (jj_done) return true;
    if (jj_scan_token(570)) return true;
    if (jj_3R_multiset_element_2657_5_353()) return true;
    return false;
  }

 inline bool jj_3_846()
 {
    if (jj_done) return true;
    if (jj_scan_token(DISTINCT)) return true;
    return false;
  }

 inline bool jj_3R_table_value_constructor_by_query_2669_5_352()
 {
    if (jj_done) return true;
    if (jj_scan_token(TABLE)) return true;
    if (jj_3R_subquery_3527_5_181()) return true;
    return false;
  }

 inline bool jj_3_847()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_845()) {
    jj_scanpos = xsp;
    if (jj_3_846()) return true;
    }
    return false;
  }

 inline bool jj_3_845()
 {
    if (jj_done) return true;
    if (jj_scan_token(ALL)) return true;
    return false;
  }

 inline bool jj_3R_multiset_value_constructor_by_query_2663_5_351()
 {
    if (jj_done) return true;
    if (jj_scan_token(MULTISET)) return true;
    if (jj_3R_subquery_3527_5_181()) return true;
    return false;
  }

 inline bool jj_3_837()
 {
    if (jj_done) return true;
    if (jj_scan_token(DISTINCT)) return true;
    return false;
  }

 inline bool jj_3R_multiset_element_2657_5_353()
 {
    if (jj_done) return true;
    if (jj_3R_value_expression_1855_5_178()) return true;
    return false;
  }

 inline bool jj_3_838()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_836()) {
    jj_scanpos = xsp;
    if (jj_3_837()) return true;
    }
    return false;
  }

 inline bool jj_3_836()
 {
    if (jj_done) return true;
    if (jj_scan_token(ALL)) return true;
    return false;
  }

 inline bool jj_3R_multiset_element_list_2651_5_919()
 {
    if (jj_done) return true;
    if (jj_3R_multiset_element_2657_5_353()) return true;
    Token * xsp;
    while (true) {
      xsp = jj_scanpos;
      if (jj_3_854()) { jj_scanpos = xsp; break; }
    }
    return false;
  }

 inline bool jj_3R_multiset_value_constructor_by_enumeration_2645_5_350()
 {
    if (jj_done) return true;
    if (jj_scan_token(MULTISET)) return true;
    if (jj_3R_left_bracket_or_trigraph_804_5_174()) return true;
    if (jj_3R_multiset_element_list_2651_5_919()) return true;
    if (jj_3R_right_bracket_or_trigraph_811_5_175()) return true;
    return false;
  }

 inline bool jj_3_853()
 {
    if (jj_done) return true;
    if (jj_3R_table_value_constructor_by_query_2669_5_352()) return true;
    return false;
  }

 inline bool jj_3_848()
 {
    if (jj_done) return true;
    if (jj_scan_token(MULTISET)) return true;
    if (jj_scan_token(INTERSECT)) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_847()) jj_scanpos = xsp;
    if (jj_3R_multiset_primary_2624_5_347()) return true;
    return false;
  }

 inline bool jj_3_852()
 {
    if (jj_done) return true;
    if (jj_3R_multiset_value_constructor_by_query_2663_5_351()) return true;
    return false;
  }

 inline bool jj_3_851()
 {
    if (jj_done) return true;
    if (jj_3R_multiset_value_constructor_by_enumeration_2645_5_350()) return true;
    return false;
  }

 inline bool jj_3R_multiset_value_constructor_2637_5_202()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_851()) {
    jj_scanpos = xsp;
    if (jj_3_852()) {
    jj_scanpos = xsp;
    if (jj_3_853()) return true;
    }
    }
    return false;
  }

 inline bool jj_3_842()
 {
    if (jj_done) return true;
    if (jj_scan_token(MULTISET)) return true;
    if (jj_scan_token(UNION)) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_838()) jj_scanpos = xsp;
    return false;
  }

 inline bool jj_3R_multiset_set_function_2631_5_348()
 {
    if (jj_done) return true;
    if (jj_scan_token(SET)) return true;
    if (jj_scan_token(lparen)) return true;
    if (jj_3R_multiset_value_expression_2612_5_269()) return true;
    if (jj_scan_token(rparen)) return true;
    return false;
  }

 inline bool jj_3_844()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_842()) {
    jj_scanpos = xsp;
    if (jj_3_843()) return true;
    }
    if (jj_3R_multiset_term_2618_5_346()) return true;
    return false;
  }

 inline bool jj_3_850()
 {
    if (jj_done) return true;
    if (jj_3R_value_expression_primary_1315_5_349()) return true;
    return false;
  }

 inline bool jj_3R_multiset_primary_2624_5_347()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_849()) {
    jj_scanpos = xsp;
    if (jj_3_850()) return true;
    }
    return false;
  }

 inline bool jj_3_849()
 {
    if (jj_done) return true;
    if (jj_3R_multiset_set_function_2631_5_348()) return true;
    return false;
  }

 inline bool jj_3R_multiset_term_2618_5_346()
 {
    if (jj_done) return true;
    if (jj_3R_multiset_primary_2624_5_347()) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_848()) jj_scanpos = xsp;
    return false;
  }

 inline bool jj_3R_multiset_value_expression_2612_5_269()
 {
    if (jj_done) return true;
    if (jj_3R_multiset_term_2618_5_346()) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_844()) jj_scanpos = xsp;
    return false;
  }

 inline bool jj_3_835()
 {
    if (jj_done) return true;
    if (jj_scan_token(570)) return true;
    if (jj_3R_array_element_2600_5_345()) return true;
    return false;
  }

 inline bool jj_3R_array_value_constructor_by_query_2606_5_343()
 {
    if (jj_done) return true;
    if (jj_scan_token(ARRAY)) return true;
    if (jj_3R_subquery_3527_5_181()) return true;
    return false;
  }

 inline bool jj_3R_array_element_2600_5_345()
 {
    if (jj_done) return true;
    if (jj_3R_value_expression_1855_5_178()) return true;
    return false;
  }

 inline bool jj_3R_array_element_list_2594_5_344()
 {
    if (jj_done) return true;
    if (jj_3R_array_element_2600_5_345()) return true;
    Token * xsp;
    while (true) {
      xsp = jj_scanpos;
      if (jj_3_835()) { jj_scanpos = xsp; break; }
    }
    return false;
  }

 inline bool jj_3_834()
 {
    if (jj_done) return true;
    if (jj_3R_array_element_list_2594_5_344()) return true;
    return false;
  }

 inline bool jj_3R_array_value_constructor_by_enumeration_2586_5_342()
 {
    if (jj_done) return true;
    if (jj_scan_token(ARRAY)) return true;
    if (jj_3R_left_bracket_or_trigraph_804_5_174()) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_834()) jj_scanpos = xsp;
    if (jj_3R_right_bracket_or_trigraph_811_5_175()) return true;
    return false;
  }

 inline bool jj_3_833()
 {
    if (jj_done) return true;
    if (jj_3R_array_value_constructor_by_query_2606_5_343()) return true;
    return false;
  }

 inline bool jj_3_832()
 {
    if (jj_done) return true;
    if (jj_3R_array_value_constructor_by_enumeration_2586_5_342()) return true;
    return false;
  }

 inline bool jj_3R_array_value_constructor_2579_5_201()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_832()) {
    jj_scanpos = xsp;
    if (jj_3_833()) return true;
    }
    return false;
  }

 inline bool jj_3R_trim_array_function_2573_5_918()
 {
    if (jj_done) return true;
    if (jj_scan_token(TRIM_ARRAY)) return true;
    if (jj_scan_token(lparen)) return true;
    if (jj_3R_array_value_expression_2548_5_268()) return true;
    if (jj_scan_token(570)) return true;
    if (jj_3R_numeric_value_expression_1894_5_261()) return true;
    if (jj_scan_token(rparen)) return true;
    return false;
  }

 inline bool jj_3R_array_value_function_2567_5_341()
 {
    if (jj_done) return true;
    if (jj_3R_trim_array_function_2573_5_918()) return true;
    return false;
  }

 inline bool jj_3_829()
 {
    if (jj_done) return true;
    if (jj_scan_token(576)) return true;
    if (jj_3R_array_primary_2560_5_340()) return true;
    return false;
  }

 inline bool jj_3_831()
 {
    if (jj_done) return true;
    if (jj_3R_multiset_value_expression_2612_5_269()) return true;
    return false;
  }

 inline bool jj_3R_array_primary_2560_5_340()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_830()) {
    jj_scanpos = xsp;
    if (jj_3_831()) return true;
    }
    return false;
  }

 inline bool jj_3_830()
 {
    if (jj_done) return true;
    if (jj_3R_array_value_function_2567_5_341()) return true;
    return false;
  }

 inline bool jj_3R_array_value_expression_2548_5_268()
 {
    if (jj_done) return true;
    if (jj_3R_array_primary_2560_5_340()) return true;
    Token * xsp;
    while (true) {
      xsp = jj_scanpos;
      if (jj_3_829()) { jj_scanpos = xsp; break; }
    }
    return false;
  }

 inline bool jj_3R_parenthesized_boolean_value_expression_2542_6_339()
 {
    if (jj_done) return true;
    if (jj_scan_token(lparen)) return true;
    if (jj_3R_boolean_value_expression_2494_5_258()) return true;
    if (jj_scan_token(rparen)) return true;
    return false;
  }

 inline bool jj_3_828()
 {
    if (jj_done) return true;
    if (jj_3R_numeric_value_expression_1894_5_261()) return true;
    return false;
  }

 inline bool jj_3R_boolean_predicand_2535_5_338()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_827()) {
    jj_scanpos = xsp;
    if (jj_3_828()) return true;
    }
    return false;
  }

 inline bool jj_3_827()
 {
    if (jj_done) return true;
    if (jj_3R_parenthesized_boolean_value_expression_2542_6_339()) return true;
    return false;
  }

 inline bool jj_3_826()
 {
    if (jj_done) return true;
    if (jj_3R_boolean_predicand_2535_5_338()) return true;
    return false;
  }

 inline bool jj_3R_boolean_primary_2528_5_917()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_825()) {
    jj_scanpos = xsp;
    if (jj_3_826()) return true;
    }
    return false;
  }

 inline bool jj_3_825()
 {
    if (jj_done) return true;
    if (jj_3R_predicate_3533_5_337()) return true;
    return false;
  }

 inline bool jj_3_824()
 {
    if (jj_done) return true;
    if (jj_scan_token(UNKNOWN)) return true;
    return false;
  }

 inline bool jj_3_820()
 {
    if (jj_done) return true;
    if (jj_scan_token(NOT)) return true;
    return false;
  }

 inline bool jj_3_823()
 {
    if (jj_done) return true;
    if (jj_scan_token(FALSE)) return true;
    return false;
  }

 inline bool jj_3R_truth_value_2520_5_336()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_822()) {
    jj_scanpos = xsp;
    if (jj_3_823()) {
    jj_scanpos = xsp;
    if (jj_3_824()) return true;
    }
    }
    return false;
  }

 inline bool jj_3_822()
 {
    if (jj_done) return true;
    if (jj_scan_token(TRUE)) return true;
    return false;
  }

 inline bool jj_3_817()
 {
    if (jj_done) return true;
    if (jj_scan_token(AND)) return true;
    if (jj_3R_boolean_factor_2506_5_334()) return true;
    return false;
  }

 inline bool jj_3_821()
 {
    if (jj_done) return true;
    if (jj_scan_token(IS)) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_820()) jj_scanpos = xsp;
    if (jj_3R_truth_value_2520_5_336()) return true;
    return false;
  }

 inline bool jj_3R_boolean_test_2513_5_335()
 {
    if (jj_done) return true;
    if (jj_3R_boolean_primary_2528_5_917()) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_821()) jj_scanpos = xsp;
    return false;
  }

 inline bool jj_3_816()
 {
    if (jj_done) return true;
    if (jj_scan_token(OR)) return true;
    if (jj_3R_boolean_term_2500_5_333()) return true;
    return false;
  }

 inline bool jj_3_819()
 {
    if (jj_done) return true;
    if (jj_3R_boolean_test_2513_5_335()) return true;
    return false;
  }

 inline bool jj_3R_boolean_factor_2506_5_334()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_818()) {
    jj_scanpos = xsp;
    if (jj_3_819()) return true;
    }
    return false;
  }

 inline bool jj_3_818()
 {
    if (jj_done) return true;
    if (jj_scan_token(NOT)) return true;
    if (jj_3R_boolean_test_2513_5_335()) return true;
    return false;
  }

 inline bool jj_3R_boolean_term_2500_5_333()
 {
    if (jj_done) return true;
    if (jj_3R_boolean_factor_2506_5_334()) return true;
    Token * xsp;
    while (true) {
      xsp = jj_scanpos;
      if (jj_3_817()) { jj_scanpos = xsp; break; }
    }
    return false;
  }

 inline bool jj_3R_boolean_value_expression_2494_5_258()
 {
    if (jj_done) return true;
    if (jj_3R_boolean_term_2500_5_333()) return true;
    Token * xsp;
    while (true) {
      xsp = jj_scanpos;
      if (jj_3_816()) { jj_scanpos = xsp; break; }
    }
    return false;
  }

 inline bool jj_3R_interval_absolute_value_function_2488_5_916()
 {
    if (jj_done) return true;
    if (jj_scan_token(ABS)) return true;
    if (jj_scan_token(lparen)) return true;
    if (jj_3R_interval_value_expression_2449_5_264()) return true;
    if (jj_scan_token(rparen)) return true;
    return false;
  }

 inline bool jj_3_807()
 {
    if (jj_done) return true;
    if (jj_scan_token(DIV)) return true;
    return false;
  }

 inline bool jj_3R_interval_value_function_2482_5_332()
 {
    if (jj_done) return true;
    if (jj_3R_interval_absolute_value_function_2488_5_916()) return true;
    return false;
  }

 inline bool jj_3_806()
 {
    if (jj_done) return true;
    if (jj_scan_token(STAR)) return true;
    return false;
  }

 inline bool jj_3_808()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_806()) {
    jj_scanpos = xsp;
    if (jj_3_807()) return true;
    }
    if (jj_3R_factor_1915_5_271()) return true;
    return false;
  }

 inline bool jj_3_813()
 {
    if (jj_done) return true;
    if (jj_3R_interval_qualifier_3874_5_331()) return true;
    return false;
  }

 inline bool jj_3_810()
 {
    if (jj_done) return true;
    if (jj_scan_token(MINUS)) return true;
    return false;
  }

 inline bool jj_3R_interval_primary_2472_5_325()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_814()) {
    jj_scanpos = xsp;
    if (jj_3_815()) return true;
    }
    return false;
  }

 inline bool jj_3_814()
 {
    if (jj_done) return true;
    if (jj_3R_interval_value_function_2482_5_332()) return true;
    return false;
  }

 inline bool jj_3_815()
 {
    if (jj_done) return true;
    if (jj_3R_array_value_expression_2548_5_268()) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_813()) jj_scanpos = xsp;
    return false;
  }

 inline bool jj_3_809()
 {
    if (jj_done) return true;
    if (jj_scan_token(PLUS)) return true;
    return false;
  }

 inline bool jj_3_812()
 {
    if (jj_done) return true;
    if (jj_3R_interval_primary_2472_5_325()) return true;
    return false;
  }

 inline bool jj_3R_interval_factor_2465_5_915()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_811()) {
    jj_scanpos = xsp;
    if (jj_3_812()) return true;
    }
    return false;
  }

 inline bool jj_3_811()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_809()) {
    jj_scanpos = xsp;
    if (jj_3_810()) return true;
    }
    if (jj_3R_interval_primary_2472_5_325()) return true;
    return false;
  }

 inline bool jj_3_803()
 {
    if (jj_done) return true;
    if (jj_scan_token(lparen)) return true;
    if (jj_scan_token(unsigned_integer)) return true;
    if (jj_scan_token(rparen)) return true;
    return false;
  }

 inline bool jj_3_805()
 {
    if (jj_done) return true;
    if (jj_scan_token(lparen)) return true;
    if (jj_3R_datetime_value_expression_2368_5_263()) return true;
    if (jj_scan_token(MINUS)) return true;
    if (jj_3R_datetime_term_2375_5_322()) return true;
    if (jj_scan_token(rparen)) return true;
    return false;
  }

 inline bool jj_3_802()
 {
    if (jj_done) return true;
    if (jj_scan_token(lparen)) return true;
    if (jj_scan_token(unsigned_integer)) return true;
    if (jj_scan_token(rparen)) return true;
    return false;
  }

 inline bool jj_3R_interval_term_2458_5_330()
 {
    if (jj_done) return true;
    if (jj_3R_interval_factor_2465_5_915()) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_808()) jj_scanpos = xsp;
    return false;
  }

 inline bool jj_3R_interval_value_expression_2450_5_907()
 {
    if (jj_done) return true;
    if (jj_scan_token(lparen)) return true;
    if (jj_3R_datetime_value_expression_2368_5_263()) return true;
    if (jj_scan_token(MINUS)) return true;
    if (jj_3R_datetime_term_2375_5_322()) return true;
    if (jj_scan_token(rparen)) return true;
    if (jj_3R_interval_qualifier_3874_5_331()) return true;
    return false;
  }

 inline bool jj_3R_interval_value_expression_2449_5_264()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_804()) {
    jj_scanpos = xsp;
    if (jj_3R_interval_value_expression_2450_5_907()) return true;
    }
    return false;
  }

 inline bool jj_3_804()
 {
    if (jj_done) return true;
    if (jj_3R_interval_term_2458_5_330()) return true;
    return false;
  }

 inline bool jj_3_801()
 {
    if (jj_done) return true;
    if (jj_scan_token(lparen)) return true;
    if (jj_scan_token(unsigned_integer)) return true;
    if (jj_scan_token(rparen)) return true;
    return false;
  }

 inline bool jj_3R_current_local_timestamp_value_function_2443_5_329()
 {
    if (jj_done) return true;
    if (jj_scan_token(LOCALTIMESTAMP)) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_803()) jj_scanpos = xsp;
    return false;
  }

 inline bool jj_3_800()
 {
    if (jj_done) return true;
    if (jj_scan_token(lparen)) return true;
    if (jj_scan_token(unsigned_integer)) return true;
    if (jj_scan_token(rparen)) return true;
    return false;
  }

 inline bool jj_3R_current_timestamp_value_function_2437_5_327()
 {
    if (jj_done) return true;
    if (jj_scan_token(CURRENT_TIMESTAMP)) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_802()) jj_scanpos = xsp;
    return false;
  }

 inline bool jj_3R_current_local_time_value_function_2431_5_328()
 {
    if (jj_done) return true;
    if (jj_scan_token(LOCALTIME)) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_801()) jj_scanpos = xsp;
    return false;
  }

 inline bool jj_3R_current_time_value_function_2425_5_326()
 {
    if (jj_done) return true;
    if (jj_scan_token(CURRENT_TIME)) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_800()) jj_scanpos = xsp;
    return false;
  }

 inline bool jj_3_799()
 {
    if (jj_done) return true;
    if (jj_3R_current_local_timestamp_value_function_2443_5_329()) return true;
    return false;
  }

 inline bool jj_3_798()
 {
    if (jj_done) return true;
    if (jj_3R_current_local_time_value_function_2431_5_328()) return true;
    return false;
  }

 inline bool jj_3_797()
 {
    if (jj_done) return true;
    if (jj_3R_current_timestamp_value_function_2437_5_327()) return true;
    return false;
  }

 inline bool jj_3_796()
 {
    if (jj_done) return true;
    if (jj_3R_current_time_value_function_2425_5_326()) return true;
    return false;
  }

 inline bool jj_3_795()
 {
    if (jj_done) return true;
    if (jj_scan_token(239)) return true;
    return false;
  }

 inline bool jj_3R_datetime_value_function_2407_3_324()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_795()) {
    jj_scanpos = xsp;
    if (jj_3_796()) {
    jj_scanpos = xsp;
    if (jj_3_797()) {
    jj_scanpos = xsp;
    if (jj_3_798()) {
    jj_scanpos = xsp;
    if (jj_3_799()) return true;
    }
    }
    }
    }
    return false;
  }

 inline bool jj_3_790()
 {
    if (jj_done) return true;
    if (jj_3R_time_zone_2394_5_323()) return true;
    return false;
  }

 inline bool jj_3_794()
 {
    if (jj_done) return true;
    if (jj_scan_token(TIME)) return true;
    if (jj_scan_token(ZONE)) return true;
    if (jj_3R_interval_primary_2472_5_325()) return true;
    return false;
  }

 inline bool jj_3R_time_zone_specifier_2400_5_914()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_793()) {
    jj_scanpos = xsp;
    if (jj_3_794()) return true;
    }
    return false;
  }

 inline bool jj_3_793()
 {
    if (jj_done) return true;
    if (jj_scan_token(LOCAL)) return true;
    return false;
  }

 inline bool jj_3_787()
 {
    if (jj_done) return true;
    if (jj_scan_token(FOR)) return true;
    if (jj_3R_numeric_value_expression_1894_5_261()) return true;
    return false;
  }

 inline bool jj_3R_time_zone_2394_5_323()
 {
    if (jj_done) return true;
    if (jj_scan_token(AT)) return true;
    if (jj_3R_time_zone_specifier_2400_5_914()) return true;
    return false;
  }

 inline bool jj_3_792()
 {
    if (jj_done) return true;
    if (jj_3R_interval_value_expression_2449_5_264()) return true;
    return false;
  }

 inline bool jj_3R_datetime_primary_2387_5_1024()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_791()) {
    jj_scanpos = xsp;
    if (jj_3_792()) return true;
    }
    return false;
  }

 inline bool jj_3_791()
 {
    if (jj_done) return true;
    if (jj_3R_datetime_value_function_2407_3_324()) return true;
    return false;
  }

 inline bool jj_3R_datetime_factor_2381_5_913()
 {
    if (jj_done) return true;
    if (jj_3R_datetime_primary_2387_5_1024()) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_790()) jj_scanpos = xsp;
    return false;
  }

 inline bool jj_3R_datetime_term_2375_5_322()
 {
    if (jj_done) return true;
    if (jj_3R_datetime_factor_2381_5_913()) return true;
    return false;
  }

 inline bool jj_3_789()
 {
    if (jj_done) return true;
    if (jj_3R_interval_value_expression_2449_5_264()) return true;
    return false;
  }

 inline bool jj_3R_datetime_value_expression_2368_5_263()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_788()) {
    jj_scanpos = xsp;
    if (jj_3_789()) return true;
    }
    return false;
  }

 inline bool jj_3_788()
 {
    if (jj_done) return true;
    if (jj_3R_datetime_term_2375_5_322()) return true;
    return false;
  }

 inline bool jj_3_785()
 {
    if (jj_done) return true;
    if (jj_scan_token(FROM)) return true;
    return false;
  }

 inline bool jj_3R_binary_overlay_function_2361_5_321()
 {
    if (jj_done) return true;
    if (jj_scan_token(OVERLAY)) return true;
    if (jj_scan_token(lparen)) return true;
    if (jj_3R_binary_value_expression_2154_5_298()) return true;
    if (jj_scan_token(PLACING)) return true;
    if (jj_3R_binary_value_expression_2154_5_298()) return true;
    if (jj_scan_token(FROM)) return true;
    if (jj_3R_numeric_value_expression_1894_5_261()) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_787()) jj_scanpos = xsp;
    if (jj_scan_token(rparen)) return true;
    return false;
  }

 inline bool jj_3_784()
 {
    if (jj_done) return true;
    if (jj_scan_token(570)) return true;
    return false;
  }

 inline bool jj_3_786()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_784()) {
    jj_scanpos = xsp;
    if (jj_3_785()) return true;
    }
    if (jj_3R_binary_value_expression_2154_5_298()) return true;
    return false;
  }

 inline bool jj_3_783()
 {
    if (jj_done) return true;
    if (jj_3R_trim_specification_2283_5_316()) return true;
    return false;
  }

 inline bool jj_3R_binary_trim_operands_2350_5_912()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_783()) jj_scanpos = xsp;
    if (jj_3R_binary_value_expression_2154_5_298()) return true;
    xsp = jj_scanpos;
    if (jj_3_786()) jj_scanpos = xsp;
    return false;
  }

 inline bool jj_3R_binary_trim_function_2344_5_320()
 {
    if (jj_done) return true;
    if (jj_scan_token(TRIM)) return true;
    if (jj_scan_token(lparen)) return true;
    if (jj_3R_binary_trim_operands_2350_5_912()) return true;
    if (jj_scan_token(rparen)) return true;
    return false;
  }

 inline bool jj_3_778()
 {
    if (jj_done) return true;
    if (jj_scan_token(lparen)) return true;
    if (jj_scan_token(rparen)) return true;
    return false;
  }

 inline bool jj_3_782()
 {
    if (jj_done) return true;
    if (jj_scan_token(FOR)) return true;
    if (jj_3R_numeric_value_expression_1894_5_261()) return true;
    return false;
  }

 inline bool jj_3R_binary_substring_function_2337_5_319()
 {
    if (jj_done) return true;
    if (jj_scan_token(SUBSTRING)) return true;
    if (jj_scan_token(lparen)) return true;
    if (jj_3R_binary_value_expression_2154_5_298()) return true;
    if (jj_scan_token(FROM)) return true;
    if (jj_3R_numeric_value_expression_1894_5_261()) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_782()) jj_scanpos = xsp;
    if (jj_scan_token(rparen)) return true;
    return false;
  }

 inline bool jj_3_781()
 {
    if (jj_done) return true;
    if (jj_3R_binary_overlay_function_2361_5_321()) return true;
    return false;
  }

 inline bool jj_3_780()
 {
    if (jj_done) return true;
    if (jj_3R_binary_trim_function_2344_5_320()) return true;
    return false;
  }

 inline bool jj_3R_binary_value_function_2329_5_303()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_779()) {
    jj_scanpos = xsp;
    if (jj_3_780()) {
    jj_scanpos = xsp;
    if (jj_3_781()) return true;
    }
    }
    return false;
  }

 inline bool jj_3_779()
 {
    if (jj_done) return true;
    if (jj_3R_binary_substring_function_2337_5_319()) return true;
    return false;
  }

 inline bool jj_3_768()
 {
    if (jj_done) return true;
    if (jj_scan_token(FOR)) return true;
    if (jj_3R_numeric_value_expression_1894_5_261()) return true;
    return false;
  }

 inline bool jj_3R_specific_type_method_2322_5_314()
 {
    if (jj_done) return true;
    if (jj_scan_token(569)) return true;
    if (jj_scan_token(SPECIFICTYPE)) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_778()) jj_scanpos = xsp;
    return false;
  }

 inline bool jj_3_770()
 {
    if (jj_done) return true;
    if (jj_scan_token(570)) return true;
    if (jj_3R_normalize_function_result_length_2315_5_317()) return true;
    return false;
  }

 inline bool jj_3_777()
 {
    if (jj_done) return true;
    if (jj_3R_character_large_object_length_1210_5_162()) return true;
    return false;
  }

 inline bool jj_3R_normalize_function_result_length_2315_5_317()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_776()) {
    jj_scanpos = xsp;
    if (jj_3_777()) return true;
    }
    return false;
  }

 inline bool jj_3_776()
 {
    if (jj_done) return true;
    if (jj_3R_character_length_1197_5_160()) return true;
    return false;
  }

 inline bool jj_3_775()
 {
    if (jj_done) return true;
    if (jj_scan_token(NFKD)) return true;
    return false;
  }

 inline bool jj_3_774()
 {
    if (jj_done) return true;
    if (jj_scan_token(NFKC)) return true;
    return false;
  }

 inline bool jj_3_773()
 {
    if (jj_done) return true;
    if (jj_scan_token(NFD)) return true;
    return false;
  }

 inline bool jj_3R_normal_form_2306_5_318()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_772()) {
    jj_scanpos = xsp;
    if (jj_3_773()) {
    jj_scanpos = xsp;
    if (jj_3_774()) {
    jj_scanpos = xsp;
    if (jj_3_775()) return true;
    }
    }
    }
    return false;
  }

 inline bool jj_3_772()
 {
    if (jj_done) return true;
    if (jj_scan_token(NFC)) return true;
    return false;
  }

 inline bool jj_3_771()
 {
    if (jj_done) return true;
    if (jj_scan_token(570)) return true;
    if (jj_3R_normal_form_2306_5_318()) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_770()) jj_scanpos = xsp;
    return false;
  }

 inline bool jj_3R_normalize_function_2299_5_313()
 {
    if (jj_done) return true;
    if (jj_scan_token(NORMALIZE)) return true;
    if (jj_scan_token(lparen)) return true;
    if (jj_3R_character_value_expression_2128_5_274()) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_771()) jj_scanpos = xsp;
    if (jj_scan_token(rparen)) return true;
    return false;
  }

 inline bool jj_3_769()
 {
    if (jj_done) return true;
    if (jj_scan_token(USING)) return true;
    if (jj_3R_char_length_units_1216_5_168()) return true;
    return false;
  }

 inline bool jj_3R_character_overlay_function_2291_5_312()
 {
    if (jj_done) return true;
    if (jj_scan_token(OVERLAY)) return true;
    if (jj_scan_token(lparen)) return true;
    if (jj_3R_character_value_expression_2128_5_274()) return true;
    if (jj_scan_token(PLACING)) return true;
    if (jj_3R_character_value_expression_2128_5_274()) return true;
    if (jj_scan_token(FROM)) return true;
    if (jj_3R_numeric_value_expression_1894_5_261()) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_768()) jj_scanpos = xsp;
    xsp = jj_scanpos;
    if (jj_3_769()) jj_scanpos = xsp;
    if (jj_scan_token(rparen)) return true;
    return false;
  }

 inline bool jj_3_767()
 {
    if (jj_done) return true;
    if (jj_scan_token(BOTH)) return true;
    return false;
  }

 inline bool jj_3_763()
 {
    if (jj_done) return true;
    if (jj_scan_token(FROM)) return true;
    return false;
  }

 inline bool jj_3_766()
 {
    if (jj_done) return true;
    if (jj_scan_token(TRAILING)) return true;
    return false;
  }

 inline bool jj_3R_trim_specification_2283_5_316()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_765()) {
    jj_scanpos = xsp;
    if (jj_3_766()) {
    jj_scanpos = xsp;
    if (jj_3_767()) return true;
    }
    }
    return false;
  }

 inline bool jj_3_765()
 {
    if (jj_done) return true;
    if (jj_scan_token(LEADING)) return true;
    return false;
  }

 inline bool jj_3_762()
 {
    if (jj_done) return true;
    if (jj_scan_token(570)) return true;
    return false;
  }

 inline bool jj_3_754()
 {
    if (jj_done) return true;
    if (jj_scan_token(FLAG)) return true;
    if (jj_3R_character_value_expression_2128_5_274()) return true;
    return false;
  }

 inline bool jj_3_764()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_762()) {
    jj_scanpos = xsp;
    if (jj_3_763()) return true;
    }
    if (jj_3R_character_value_expression_2128_5_274()) return true;
    return false;
  }

 inline bool jj_3_761()
 {
    if (jj_done) return true;
    if (jj_3R_trim_specification_2283_5_316()) return true;
    return false;
  }

 inline bool jj_3R_trim_operands_2272_5_911()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_761()) jj_scanpos = xsp;
    if (jj_3R_character_value_expression_2128_5_274()) return true;
    xsp = jj_scanpos;
    if (jj_3_764()) jj_scanpos = xsp;
    return false;
  }

 inline bool jj_3R_trim_function_2266_5_311()
 {
    if (jj_done) return true;
    if (jj_scan_token(TRIM)) return true;
    if (jj_scan_token(lparen)) return true;
    if (jj_3R_trim_operands_2272_5_911()) return true;
    if (jj_scan_token(rparen)) return true;
    return false;
  }

 inline bool jj_3_760()
 {
    if (jj_done) return true;
    if (jj_3R_numeric_value_expression_1894_5_261()) return true;
    return false;
  }

 inline bool jj_3R_regex_transliteration_occurrence_2259_5_315()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_759()) {
    jj_scanpos = xsp;
    if (jj_3_760()) return true;
    }
    return false;
  }

 inline bool jj_3_759()
 {
    if (jj_done) return true;
    if (jj_scan_token(ALL)) return true;
    return false;
  }

 inline bool jj_3_758()
 {
    if (jj_done) return true;
    if (jj_scan_token(OCCURRENCE)) return true;
    if (jj_3R_regex_transliteration_occurrence_2259_5_315()) return true;
    return false;
  }

 inline bool jj_3_757()
 {
    if (jj_done) return true;
    if (jj_scan_token(USING)) return true;
    if (jj_3R_char_length_units_1216_5_168()) return true;
    return false;
  }

 inline bool jj_3_756()
 {
    if (jj_done) return true;
    if (jj_scan_token(FROM)) return true;
    if (jj_3R_numeric_value_expression_1894_5_261()) return true;
    return false;
  }

 inline bool jj_3_755()
 {
    if (jj_done) return true;
    if (jj_scan_token(WITH)) return true;
    if (jj_3R_character_value_expression_2128_5_274()) return true;
    return false;
  }

 inline bool jj_3_747()
 {
    if (jj_done) return true;
    if (jj_scan_token(FLAG)) return true;
    if (jj_3R_character_value_expression_2128_5_274()) return true;
    return false;
  }

 inline bool jj_3R_regex_transliteration_2246_5_310()
 {
    if (jj_done) return true;
    if (jj_scan_token(TRANSLATE_REGEX)) return true;
    if (jj_scan_token(lparen)) return true;
    if (jj_3R_character_value_expression_2128_5_274()) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_754()) jj_scanpos = xsp;
    if (jj_scan_token(IN)) return true;
    if (jj_3R_character_value_expression_2128_5_274()) return true;
    xsp = jj_scanpos;
    if (jj_3_755()) jj_scanpos = xsp;
    xsp = jj_scanpos;
    if (jj_3_756()) jj_scanpos = xsp;
    xsp = jj_scanpos;
    if (jj_3_757()) jj_scanpos = xsp;
    xsp = jj_scanpos;
    if (jj_3_758()) jj_scanpos = xsp;
    if (jj_scan_token(rparen)) return true;
    return false;
  }

 inline bool jj_3R_character_transliteration_2240_5_309()
 {
    if (jj_done) return true;
    if (jj_scan_token(TRANSLATE)) return true;
    if (jj_scan_token(lparen)) return true;
    if (jj_3R_character_value_expression_2128_5_274()) return true;
    if (jj_scan_token(USING)) return true;
    if (jj_3R_schema_qualified_name_970_5_252()) return true;
    if (jj_scan_token(rparen)) return true;
    return false;
  }

 inline bool jj_3_753()
 {
    if (jj_done) return true;
    if (jj_scan_token(LOWER)) return true;
    return false;
  }

 inline bool jj_3R_transcoding_2234_5_308()
 {
    if (jj_done) return true;
    if (jj_scan_token(CONVERT)) return true;
    if (jj_scan_token(lparen)) return true;
    if (jj_3R_character_value_expression_2128_5_274()) return true;
    if (jj_scan_token(USING)) return true;
    if (jj_3R_schema_qualified_name_970_5_252()) return true;
    if (jj_scan_token(rparen)) return true;
    return false;
  }

 inline bool jj_3_752()
 {
    if (jj_done) return true;
    if (jj_scan_token(UPPER)) return true;
    return false;
  }

 inline bool jj_3R_fold_2228_5_307()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_752()) {
    jj_scanpos = xsp;
    if (jj_3_753()) return true;
    }
    if (jj_scan_token(lparen)) return true;
    if (jj_3R_character_value_expression_2128_5_274()) return true;
    if (jj_scan_token(rparen)) return true;
    return false;
  }

 inline bool jj_3_751()
 {
    if (jj_done) return true;
    if (jj_scan_token(GROUP)) return true;
    if (jj_3R_numeric_value_expression_1894_5_261()) return true;
    return false;
  }

 inline bool jj_3_750()
 {
    if (jj_done) return true;
    if (jj_scan_token(OCCURRENCE)) return true;
    if (jj_3R_numeric_value_expression_1894_5_261()) return true;
    return false;
  }

 inline bool jj_3_749()
 {
    if (jj_done) return true;
    if (jj_scan_token(USING)) return true;
    if (jj_3R_char_length_units_1216_5_168()) return true;
    return false;
  }

 inline bool jj_3_748()
 {
    if (jj_done) return true;
    if (jj_scan_token(FROM)) return true;
    if (jj_3R_numeric_value_expression_1894_5_261()) return true;
    return false;
  }

 inline bool jj_3R_regex_substring_function_2215_5_306()
 {
    if (jj_done) return true;
    if (jj_scan_token(SUBSTRING_REGEX)) return true;
    if (jj_scan_token(lparen)) return true;
    if (jj_3R_character_value_expression_2128_5_274()) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_747()) jj_scanpos = xsp;
    if (jj_scan_token(IN)) return true;
    if (jj_3R_character_value_expression_2128_5_274()) return true;
    xsp = jj_scanpos;
    if (jj_3_748()) jj_scanpos = xsp;
    xsp = jj_scanpos;
    if (jj_3_749()) jj_scanpos = xsp;
    xsp = jj_scanpos;
    if (jj_3_750()) jj_scanpos = xsp;
    xsp = jj_scanpos;
    if (jj_3_751()) jj_scanpos = xsp;
    if (jj_scan_token(rparen)) return true;
    return false;
  }

 inline bool jj_3R_regular_expression_substring_function_2208_5_305()
 {
    if (jj_done) return true;
    if (jj_scan_token(SUBSTRING)) return true;
    if (jj_scan_token(lparen)) return true;
    if (jj_3R_character_value_expression_2128_5_274()) return true;
    if (jj_scan_token(SIMILAR)) return true;
    if (jj_3R_character_value_expression_2128_5_274()) return true;
    if (jj_scan_token(ESCAPE)) return true;
    if (jj_3R_character_value_expression_2128_5_274()) return true;
    if (jj_scan_token(rparen)) return true;
    return false;
  }

 inline bool jj_3_746()
 {
    if (jj_done) return true;
    if (jj_scan_token(USING)) return true;
    if (jj_3R_char_length_units_1216_5_168()) return true;
    return false;
  }

 inline bool jj_3_745()
 {
    if (jj_done) return true;
    if (jj_scan_token(FOR)) return true;
    if (jj_3R_numeric_value_expression_1894_5_261()) return true;
    return false;
  }

 inline bool jj_3R_character_substring_function_2199_5_304()
 {
    if (jj_done) return true;
    if (jj_scan_token(SUBSTRING)) return true;
    if (jj_scan_token(lparen)) return true;
    if (jj_3R_character_value_expression_2128_5_274()) return true;
    if (jj_scan_token(FROM)) return true;
    if (jj_3R_numeric_value_expression_1894_5_261()) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_745()) jj_scanpos = xsp;
    xsp = jj_scanpos;
    if (jj_3_746()) jj_scanpos = xsp;
    if (jj_scan_token(rparen)) return true;
    return false;
  }

 inline bool jj_3_744()
 {
    if (jj_done) return true;
    if (jj_3R_specific_type_method_2322_5_314()) return true;
    return false;
  }

 inline bool jj_3_743()
 {
    if (jj_done) return true;
    if (jj_3R_normalize_function_2299_5_313()) return true;
    return false;
  }

 inline bool jj_3_742()
 {
    if (jj_done) return true;
    if (jj_3R_character_overlay_function_2291_5_312()) return true;
    return false;
  }

 inline bool jj_3_741()
 {
    if (jj_done) return true;
    if (jj_3R_trim_function_2266_5_311()) return true;
    return false;
  }

 inline bool jj_3_740()
 {
    if (jj_done) return true;
    if (jj_3R_regex_transliteration_2246_5_310()) return true;
    return false;
  }

 inline bool jj_3_739()
 {
    if (jj_done) return true;
    if (jj_3R_character_transliteration_2240_5_309()) return true;
    return false;
  }

 inline bool jj_3_738()
 {
    if (jj_done) return true;
    if (jj_3R_transcoding_2234_5_308()) return true;
    return false;
  }

 inline bool jj_3_737()
 {
    if (jj_done) return true;
    if (jj_3R_fold_2228_5_307()) return true;
    return false;
  }

 inline bool jj_3_736()
 {
    if (jj_done) return true;
    if (jj_3R_regex_substring_function_2215_5_306()) return true;
    return false;
  }

 inline bool jj_3_735()
 {
    if (jj_done) return true;
    if (jj_3R_regular_expression_substring_function_2208_5_305()) return true;
    return false;
  }

 inline bool jj_3_734()
 {
    if (jj_done) return true;
    if (jj_3R_character_substring_function_2199_5_304()) return true;
    return false;
  }

 inline bool jj_3R_character_value_function_2181_3_302()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_734()) {
    jj_scanpos = xsp;
    if (jj_3_735()) {
    jj_scanpos = xsp;
    if (jj_3_736()) {
    jj_scanpos = xsp;
    if (jj_3_737()) {
    jj_scanpos = xsp;
    if (jj_3_738()) {
    jj_scanpos = xsp;
    if (jj_3_739()) {
    jj_scanpos = xsp;
    if (jj_3_740()) {
    jj_scanpos = xsp;
    if (jj_3_741()) {
    jj_scanpos = xsp;
    if (jj_3_742()) {
    jj_scanpos = xsp;
    if (jj_3_743()) {
    jj_scanpos = xsp;
    if (jj_3_744()) return true;
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    return false;
  }

 inline bool jj_3_733()
 {
    if (jj_done) return true;
    if (jj_3R_binary_value_function_2329_5_303()) return true;
    return false;
  }

 inline bool jj_3R_string_value_function_2174_5_300()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_732()) {
    jj_scanpos = xsp;
    if (jj_3_733()) return true;
    }
    return false;
  }

 inline bool jj_3_732()
 {
    if (jj_done) return true;
    if (jj_3R_character_value_function_2181_3_302()) return true;
    return false;
  }

 inline bool jj_3_729()
 {
    if (jj_done) return true;
    if (jj_3R_binary_concatenation_2168_5_301()) return true;
    return false;
  }

 inline bool jj_3R_binary_concatenation_2168_5_301()
 {
    if (jj_done) return true;
    if (jj_scan_token(576)) return true;
    if (jj_3R_binary_primary_2160_5_910()) return true;
    return false;
  }

 inline bool jj_3_726()
 {
    if (jj_done) return true;
    if (jj_3R_collate_clause_4076_5_153()) return true;
    return false;
  }

 inline bool jj_3_731()
 {
    if (jj_done) return true;
    if (jj_3R_datetime_value_expression_2368_5_263()) return true;
    return false;
  }

 inline bool jj_3R_binary_primary_2160_5_910()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_730()) {
    jj_scanpos = xsp;
    if (jj_3_731()) return true;
    }
    return false;
  }

 inline bool jj_3_730()
 {
    if (jj_done) return true;
    if (jj_3R_string_value_function_2174_5_300()) return true;
    return false;
  }

 inline bool jj_3R_binary_value_expression_2154_5_298()
 {
    if (jj_done) return true;
    if (jj_3R_binary_primary_2160_5_910()) return true;
    Token * xsp;
    while (true) {
      xsp = jj_scanpos;
      if (jj_3_729()) { jj_scanpos = xsp; break; }
    }
    return false;
  }

 inline bool jj_3_725()
 {
    if (jj_done) return true;
    if (jj_3R_concatenation_2135_5_299()) return true;
    return false;
  }

 inline bool jj_3_728()
 {
    if (jj_done) return true;
    if (jj_3R_binary_value_expression_2154_5_298()) return true;
    return false;
  }

 inline bool jj_3R_character_primary_2147_5_1023()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_727()) {
    jj_scanpos = xsp;
    if (jj_3_728()) return true;
    }
    return false;
  }

 inline bool jj_3_727()
 {
    if (jj_done) return true;
    if (jj_3R_string_value_function_2174_5_300()) return true;
    return false;
  }

 inline bool jj_3R_character_factor_2141_5_908()
 {
    if (jj_done) return true;
    if (jj_3R_character_primary_2147_5_1023()) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_726()) jj_scanpos = xsp;
    return false;
  }

 inline bool jj_3R_concatenation_2135_5_299()
 {
    if (jj_done) return true;
    if (jj_scan_token(576)) return true;
    if (jj_3R_character_factor_2141_5_908()) return true;
    return false;
  }

 inline bool jj_3R_character_value_expression_2128_5_274()
 {
    if (jj_done) return true;
    if (jj_3R_character_factor_2141_5_908()) return true;
    Token * xsp;
    while (true) {
      xsp = jj_scanpos;
      if (jj_3_725()) { jj_scanpos = xsp; break; }
    }
    return false;
  }

 inline bool jj_3_724()
 {
    if (jj_done) return true;
    if (jj_3R_binary_value_expression_2154_5_298()) return true;
    return false;
  }

 inline bool jj_3R_string_value_expression_2121_5_262()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_723()) {
    jj_scanpos = xsp;
    if (jj_3_724()) return true;
    }
    return false;
  }

 inline bool jj_3_723()
 {
    if (jj_done) return true;
    if (jj_3R_character_value_expression_2128_5_274()) return true;
    return false;
  }

 inline bool jj_3_721()
 {
    if (jj_done) return true;
    if (jj_scan_token(CEILING)) return true;
    return false;
  }

 inline bool jj_3_722()
 {
    if (jj_done) return true;
    if (jj_scan_token(570)) return true;
    if (jj_3R_numeric_value_expression_1894_5_261()) return true;
    if (jj_scan_token(570)) return true;
    if (jj_3R_numeric_value_expression_1894_5_261()) return true;
    return false;
  }

 inline bool jj_3R_width_bucket_function_2113_5_290()
 {
    if (jj_done) return true;
    if (jj_scan_token(WIDTH_BUCKET)) return true;
    if (jj_scan_token(lparen)) return true;
    if (jj_3R_numeric_value_expression_1894_5_261()) return true;
    if (jj_scan_token(570)) return true;
    if (jj_3R_numeric_value_expression_1894_5_261()) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_722()) jj_scanpos = xsp;
    if (jj_scan_token(rparen)) return true;
    return false;
  }

 inline bool jj_3_720()
 {
    if (jj_done) return true;
    if (jj_scan_token(CEIL)) return true;
    return false;
  }

 inline bool jj_3R_ceiling_function_2107_5_289()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_720()) {
    jj_scanpos = xsp;
    if (jj_3_721()) return true;
    }
    if (jj_scan_token(lparen)) return true;
    if (jj_3R_numeric_value_expression_1894_5_261()) return true;
    if (jj_scan_token(rparen)) return true;
    return false;
  }

 inline bool jj_3R_floor_function_2101_5_288()
 {
    if (jj_done) return true;
    if (jj_scan_token(FLOOR)) return true;
    if (jj_scan_token(lparen)) return true;
    if (jj_3R_numeric_value_expression_1894_5_261()) return true;
    if (jj_scan_token(rparen)) return true;
    return false;
  }

 inline bool jj_3R_square_root_2095_5_287()
 {
    if (jj_done) return true;
    if (jj_scan_token(SQRT)) return true;
    if (jj_scan_token(lparen)) return true;
    if (jj_3R_numeric_value_expression_1894_5_261()) return true;
    if (jj_scan_token(rparen)) return true;
    return false;
  }

 inline bool jj_3R_power_function_2089_5_286()
 {
    if (jj_done) return true;
    if (jj_scan_token(POWER)) return true;
    if (jj_scan_token(lparen)) return true;
    if (jj_3R_numeric_value_expression_1894_5_261()) return true;
    if (jj_scan_token(570)) return true;
    if (jj_3R_numeric_value_expression_1894_5_261()) return true;
    if (jj_scan_token(rparen)) return true;
    return false;
  }

 inline bool jj_3R_exponential_function_2083_5_285()
 {
    if (jj_done) return true;
    if (jj_scan_token(EXP)) return true;
    if (jj_scan_token(lparen)) return true;
    if (jj_3R_numeric_value_expression_1894_5_261()) return true;
    if (jj_scan_token(rparen)) return true;
    return false;
  }

 inline bool jj_3R_natural_logarithm_2077_5_284()
 {
    if (jj_done) return true;
    if (jj_scan_token(LN)) return true;
    if (jj_scan_token(lparen)) return true;
    if (jj_3R_numeric_value_expression_1894_5_261()) return true;
    if (jj_scan_token(rparen)) return true;
    return false;
  }

 inline bool jj_3R_modulus_expression_2071_5_283()
 {
    if (jj_done) return true;
    if (jj_scan_token(MOD)) return true;
    if (jj_scan_token(lparen)) return true;
    if (jj_3R_numeric_value_expression_1894_5_261()) return true;
    if (jj_scan_token(570)) return true;
    if (jj_3R_numeric_value_expression_1894_5_261()) return true;
    if (jj_scan_token(rparen)) return true;
    return false;
  }

 inline bool jj_3R_absolute_value_expression_2065_5_282()
 {
    if (jj_done) return true;
    if (jj_scan_token(ABS)) return true;
    if (jj_scan_token(lparen)) return true;
    if (jj_3R_numeric_value_expression_1894_5_261()) return true;
    if (jj_scan_token(rparen)) return true;
    return false;
  }

 inline bool jj_3R_max_cardinality_expression_2059_5_281()
 {
    if (jj_done) return true;
    if (jj_scan_token(MAX_CARDINALITY)) return true;
    if (jj_scan_token(lparen)) return true;
    if (jj_3R_array_value_expression_2548_5_268()) return true;
    if (jj_scan_token(rparen)) return true;
    return false;
  }

 inline bool jj_3R_cardinality_expression_2053_5_280()
 {
    if (jj_done) return true;
    if (jj_scan_token(CARDINALITY)) return true;
    if (jj_scan_token(lparen)) return true;
    if (jj_3R_collection_value_expression_1887_5_267()) return true;
    if (jj_scan_token(rparen)) return true;
    return false;
  }

 inline bool jj_3_719()
 {
    if (jj_done) return true;
    if (jj_3R_interval_value_expression_2449_5_264()) return true;
    return false;
  }

 inline bool jj_3R_extract_source_2046_5_1041()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_718()) {
    jj_scanpos = xsp;
    if (jj_3_719()) return true;
    }
    return false;
  }

 inline bool jj_3_718()
 {
    if (jj_done) return true;
    if (jj_3R_datetime_value_expression_2368_5_263()) return true;
    return false;
  }

 inline bool jj_3_717()
 {
    if (jj_done) return true;
    if (jj_scan_token(TIMEZONE_MINUTE)) return true;
    return false;
  }

 inline bool jj_3R_time_zone_field_2039_5_297()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_716()) {
    jj_scanpos = xsp;
    if (jj_3_717()) return true;
    }
    return false;
  }

 inline bool jj_3_716()
 {
    if (jj_done) return true;
    if (jj_scan_token(TIMEZONE_HOUR)) return true;
    return false;
  }

 inline bool jj_3_715()
 {
    if (jj_done) return true;
    if (jj_3R_time_zone_field_2039_5_297()) return true;
    return false;
  }

 inline bool jj_3R_extract_field_2032_5_909()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_714()) {
    jj_scanpos = xsp;
    if (jj_3_715()) return true;
    }
    return false;
  }

 inline bool jj_3_714()
 {
    if (jj_done) return true;
    if (jj_3R_primary_datetime_field_3906_5_296()) return true;
    return false;
  }

 inline bool jj_3_712()
 {
    if (jj_done) return true;
    if (jj_scan_token(CHARACTER_LENGTH)) return true;
    return false;
  }

 inline bool jj_3R_extract_expression_2026_5_278()
 {
    if (jj_done) return true;
    if (jj_scan_token(EXTRACT)) return true;
    if (jj_scan_token(lparen)) return true;
    if (jj_3R_extract_field_2032_5_909()) return true;
    if (jj_scan_token(FROM)) return true;
    if (jj_3R_extract_source_2046_5_1041()) return true;
    if (jj_scan_token(rparen)) return true;
    return false;
  }

 inline bool jj_3R_octet_length_expression_2020_5_295()
 {
    if (jj_done) return true;
    if (jj_scan_token(OCTET_LENGTH)) return true;
    if (jj_scan_token(lparen)) return true;
    if (jj_3R_string_value_expression_2121_5_262()) return true;
    if (jj_scan_token(rparen)) return true;
    return false;
  }

 inline bool jj_3_713()
 {
    if (jj_done) return true;
    if (jj_scan_token(USING)) return true;
    if (jj_3R_char_length_units_1216_5_168()) return true;
    return false;
  }

 inline bool jj_3_711()
 {
    if (jj_done) return true;
    if (jj_scan_token(CHAR_LENGTH)) return true;
    return false;
  }

 inline bool jj_3R_char_length_expression_2013_5_294()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_711()) {
    jj_scanpos = xsp;
    if (jj_3_712()) return true;
    }
    if (jj_scan_token(lparen)) return true;
    if (jj_3R_character_value_expression_2128_5_274()) return true;
    xsp = jj_scanpos;
    if (jj_3_713()) jj_scanpos = xsp;
    if (jj_scan_token(rparen)) return true;
    return false;
  }

 inline bool jj_3_710()
 {
    if (jj_done) return true;
    if (jj_3R_octet_length_expression_2020_5_295()) return true;
    return false;
  }

 inline bool jj_3R_length_expression_2006_5_279()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_709()) {
    jj_scanpos = xsp;
    if (jj_3_710()) return true;
    }
    return false;
  }

 inline bool jj_3_709()
 {
    if (jj_done) return true;
    if (jj_3R_char_length_expression_2013_5_294()) return true;
    return false;
  }

 inline bool jj_3_701()
 {
    if (jj_done) return true;
    if (jj_scan_token(FLAG)) return true;
    if (jj_3R_character_value_expression_2128_5_274()) return true;
    return false;
  }

 inline bool jj_3R_binary_position_expression_2000_5_292()
 {
    if (jj_done) return true;
    if (jj_scan_token(POSITION)) return true;
    if (jj_scan_token(lparen)) return true;
    if (jj_3R_binary_value_expression_2154_5_298()) return true;
    if (jj_scan_token(IN)) return true;
    if (jj_3R_binary_value_expression_2154_5_298()) return true;
    if (jj_scan_token(rparen)) return true;
    return false;
  }

 inline bool jj_3_708()
 {
    if (jj_done) return true;
    if (jj_scan_token(USING)) return true;
    if (jj_3R_char_length_units_1216_5_168()) return true;
    return false;
  }

 inline bool jj_3R_character_position_expression_1993_5_291()
 {
    if (jj_done) return true;
    if (jj_scan_token(POSITION)) return true;
    if (jj_scan_token(lparen)) return true;
    if (jj_3R_character_value_expression_2128_5_274()) return true;
    if (jj_scan_token(IN)) return true;
    if (jj_3R_character_value_expression_2128_5_274()) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_708()) jj_scanpos = xsp;
    if (jj_scan_token(rparen)) return true;
    return false;
  }

 inline bool jj_3_697()
 {
    if (jj_done) return true;
    if (jj_scan_token(FLAG)) return true;
    if (jj_3R_character_value_expression_2128_5_274()) return true;
    return false;
  }

 inline bool jj_3_707()
 {
    if (jj_done) return true;
    if (jj_scan_token(AFTER)) return true;
    return false;
  }

 inline bool jj_3R_regex_position_start_or_after_1986_5_293()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_706()) {
    jj_scanpos = xsp;
    if (jj_3_707()) return true;
    }
    return false;
  }

 inline bool jj_3_706()
 {
    if (jj_done) return true;
    if (jj_scan_token(START)) return true;
    return false;
  }

 inline bool jj_3_705()
 {
    if (jj_done) return true;
    if (jj_scan_token(GROUP)) return true;
    if (jj_3R_numeric_value_expression_1894_5_261()) return true;
    return false;
  }

 inline bool jj_3_704()
 {
    if (jj_done) return true;
    if (jj_scan_token(OCCURRENCE)) return true;
    if (jj_3R_numeric_value_expression_1894_5_261()) return true;
    return false;
  }

 inline bool jj_3_703()
 {
    if (jj_done) return true;
    if (jj_scan_token(USING)) return true;
    if (jj_3R_char_length_units_1216_5_168()) return true;
    return false;
  }

 inline bool jj_3_702()
 {
    if (jj_done) return true;
    if (jj_scan_token(FROM)) return true;
    if (jj_3R_numeric_value_expression_1894_5_261()) return true;
    return false;
  }

 inline bool jj_3_700()
 {
    if (jj_done) return true;
    if (jj_3R_regex_position_start_or_after_1986_5_293()) return true;
    return false;
  }

 inline bool jj_3R_regex_position_expression_1972_5_277()
 {
    if (jj_done) return true;
    if (jj_scan_token(POSITION_REGEX)) return true;
    if (jj_scan_token(lparen)) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_700()) jj_scanpos = xsp;
    if (jj_3R_character_value_expression_2128_5_274()) return true;
    xsp = jj_scanpos;
    if (jj_3_701()) jj_scanpos = xsp;
    if (jj_scan_token(IN)) return true;
    if (jj_3R_character_value_expression_2128_5_274()) return true;
    xsp = jj_scanpos;
    if (jj_3_702()) jj_scanpos = xsp;
    xsp = jj_scanpos;
    if (jj_3_703()) jj_scanpos = xsp;
    xsp = jj_scanpos;
    if (jj_3_704()) jj_scanpos = xsp;
    xsp = jj_scanpos;
    if (jj_3_705()) jj_scanpos = xsp;
    if (jj_scan_token(rparen)) return true;
    return false;
  }

 inline bool jj_3_699()
 {
    if (jj_done) return true;
    if (jj_scan_token(USING)) return true;
    if (jj_3R_char_length_units_1216_5_168()) return true;
    return false;
  }

 inline bool jj_3_698()
 {
    if (jj_done) return true;
    if (jj_scan_token(FROM)) return true;
    if (jj_3R_numeric_value_expression_1894_5_261()) return true;
    return false;
  }

 inline bool jj_3R_regex_occurrences_function_1961_5_276()
 {
    if (jj_done) return true;
    if (jj_scan_token(OCCURRENCES_REGEX)) return true;
    if (jj_scan_token(lparen)) return true;
    if (jj_3R_character_value_expression_2128_5_274()) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_697()) jj_scanpos = xsp;
    if (jj_scan_token(IN)) return true;
    if (jj_3R_character_value_expression_2128_5_274()) return true;
    xsp = jj_scanpos;
    if (jj_3_698()) jj_scanpos = xsp;
    xsp = jj_scanpos;
    if (jj_3_699()) jj_scanpos = xsp;
    if (jj_scan_token(rparen)) return true;
    return false;
  }

 inline bool jj_3_2181()
 {
    if (jj_done) return true;
    if (jj_scan_token(570)) return true;
    if (jj_3R_actual_identifier_939_5_137()) return true;
    if (jj_scan_token(EQUAL)) return true;
    return false;
  }

 inline bool jj_3_696()
 {
    if (jj_done) return true;
    if (jj_3R_binary_position_expression_2000_5_292()) return true;
    return false;
  }

 inline bool jj_3R_position_expression_1954_5_275()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_695()) {
    jj_scanpos = xsp;
    if (jj_3_696()) return true;
    }
    return false;
  }

 inline bool jj_3_695()
 {
    if (jj_done) return true;
    if (jj_3R_character_position_expression_1993_5_291()) return true;
    return false;
  }

 inline bool jj_3_694()
 {
    if (jj_done) return true;
    if (jj_3R_width_bucket_function_2113_5_290()) return true;
    return false;
  }

 inline bool jj_3_693()
 {
    if (jj_done) return true;
    if (jj_3R_ceiling_function_2107_5_289()) return true;
    return false;
  }

 inline bool jj_3_692()
 {
    if (jj_done) return true;
    if (jj_3R_floor_function_2101_5_288()) return true;
    return false;
  }

 inline bool jj_3_691()
 {
    if (jj_done) return true;
    if (jj_3R_square_root_2095_5_287()) return true;
    return false;
  }

 inline bool jj_3_690()
 {
    if (jj_done) return true;
    if (jj_3R_power_function_2089_5_286()) return true;
    return false;
  }

 inline bool jj_3_689()
 {
    if (jj_done) return true;
    if (jj_3R_exponential_function_2083_5_285()) return true;
    return false;
  }

 inline bool jj_3_688()
 {
    if (jj_done) return true;
    if (jj_3R_natural_logarithm_2077_5_284()) return true;
    return false;
  }

 inline bool jj_3_687()
 {
    if (jj_done) return true;
    if (jj_3R_modulus_expression_2071_5_283()) return true;
    return false;
  }

 inline bool jj_3_686()
 {
    if (jj_done) return true;
    if (jj_3R_absolute_value_expression_2065_5_282()) return true;
    return false;
  }

 inline bool jj_3_685()
 {
    if (jj_done) return true;
    if (jj_3R_max_cardinality_expression_2059_5_281()) return true;
    return false;
  }

 inline bool jj_3_2179()
 {
    if (jj_done) return true;
    if (jj_scan_token(570)) return true;
    if (jj_3R_value_expression_1855_5_178()) return true;
    return false;
  }

 inline bool jj_3_684()
 {
    if (jj_done) return true;
    if (jj_3R_cardinality_expression_2053_5_280()) return true;
    return false;
  }

 inline bool jj_3_683()
 {
    if (jj_done) return true;
    if (jj_3R_length_expression_2006_5_279()) return true;
    return false;
  }

 inline bool jj_3_682()
 {
    if (jj_done) return true;
    if (jj_3R_extract_expression_2026_5_278()) return true;
    return false;
  }

 inline bool jj_3_681()
 {
    if (jj_done) return true;
    if (jj_3R_regex_position_expression_1972_5_277()) return true;
    return false;
  }

 inline bool jj_3_680()
 {
    if (jj_done) return true;
    if (jj_3R_regex_occurrences_function_1961_5_276()) return true;
    return false;
  }

 inline bool jj_3_679()
 {
    if (jj_done) return true;
    if (jj_3R_position_expression_1954_5_275()) return true;
    return false;
  }

 inline bool jj_3R_numeric_value_function_1929_3_273()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_679()) {
    jj_scanpos = xsp;
    if (jj_3_680()) {
    jj_scanpos = xsp;
    if (jj_3_681()) {
    jj_scanpos = xsp;
    if (jj_3_682()) {
    jj_scanpos = xsp;
    if (jj_3_683()) {
    jj_scanpos = xsp;
    if (jj_3_684()) {
    jj_scanpos = xsp;
    if (jj_3_685()) {
    jj_scanpos = xsp;
    if (jj_3_686()) {
    jj_scanpos = xsp;
    if (jj_3_687()) {
    jj_scanpos = xsp;
    if (jj_3_688()) {
    jj_scanpos = xsp;
    if (jj_3_689()) {
    jj_scanpos = xsp;
    if (jj_3_690()) {
    jj_scanpos = xsp;
    if (jj_3_691()) {
    jj_scanpos = xsp;
    if (jj_3_692()) {
    jj_scanpos = xsp;
    if (jj_3_693()) {
    jj_scanpos = xsp;
    if (jj_3_694()) return true;
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    return false;
  }

 inline bool jj_3_2182()
 {
    if (jj_done) return true;
    if (jj_scan_token(570)) return true;
    if (jj_3R_value_expression_1855_5_178()) return true;
    return false;
  }

 inline bool jj_3R_extra_args_to_agg_8069_4_492()
 {
    if (jj_done) return true;
    Token * xsp;
    if (jj_3_2182()) return true;
    while (true) {
      xsp = jj_scanpos;
      if (jj_3_2182()) { jj_scanpos = xsp; break; }
    }
    return false;
  }

 inline bool jj_3_674()
 {
    if (jj_done) return true;
    if (jj_scan_token(MINUS)) return true;
    return false;
  }

 inline bool jj_3_678()
 {
    if (jj_done) return true;
    if (jj_3R_character_value_expression_2128_5_274()) return true;
    return false;
  }

 inline bool jj_3R_numeric_primary_1922_5_272()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_677()) {
    jj_scanpos = xsp;
    if (jj_3_678()) return true;
    }
    return false;
  }

 inline bool jj_3_677()
 {
    if (jj_done) return true;
    if (jj_3R_numeric_value_function_1929_3_273()) return true;
    return false;
  }

 inline bool jj_3R_udaf_filter_8063_4_253()
 {
    if (jj_done) return true;
    if (jj_3R_filter_clause_4166_5_491()) return true;
    return false;
  }

 inline bool jj_3_673()
 {
    if (jj_done) return true;
    if (jj_scan_token(PLUS)) return true;
    return false;
  }

 inline bool jj_3_676()
 {
    if (jj_done) return true;
    if (jj_3R_numeric_primary_1922_5_272()) return true;
    return false;
  }

 inline bool jj_3R_factor_1915_5_271()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_675()) {
    jj_scanpos = xsp;
    if (jj_3_676()) return true;
    }
    return false;
  }

 inline bool jj_3_675()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_673()) {
    jj_scanpos = xsp;
    if (jj_3_674()) return true;
    }
    if (jj_3R_numeric_primary_1922_5_272()) return true;
    return false;
  }

 inline bool jj_3R_or_replace_8057_5_593()
 {
    if (jj_done) return true;
    if (jj_scan_token(OR)) return true;
    if (jj_scan_token(REPLACE)) return true;
    return false;
  }

 inline bool jj_3_2168()
 {
    if (jj_done) return true;
    if (jj_scan_token(STAR)) return true;
    return false;
  }

 inline bool jj_3_672()
 {
    if (jj_done) return true;
    if (jj_scan_token(586)) return true;
    if (jj_3R_factor_1915_5_271()) return true;
    return false;
  }

 inline bool jj_3_671()
 {
    if (jj_done) return true;
    if (jj_scan_token(DIV)) return true;
    if (jj_3R_factor_1915_5_271()) return true;
    return false;
  }

 inline bool jj_3R_table_attributes_8051_5_523()
 {
    if (jj_done) return true;
    if (jj_scan_token(lparen)) return true;
    if (jj_3R_actual_identifier_939_5_137()) return true;
    if (jj_scan_token(EQUAL)) return true;
    return false;
  }

 inline bool jj_3_670()
 {
    if (jj_done) return true;
    if (jj_scan_token(STAR)) return true;
    if (jj_3R_factor_1915_5_271()) return true;
    return false;
  }

 inline bool jj_3_669()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_670()) {
    jj_scanpos = xsp;
    if (jj_3_671()) {
    jj_scanpos = xsp;
    if (jj_3_672()) return true;
    }
    }
    return false;
  }

 inline bool jj_3R_term_1904_5_270()
 {
    if (jj_done) return true;
    if (jj_3R_factor_1915_5_271()) return true;
    Token * xsp;
    while (true) {
      xsp = jj_scanpos;
      if (jj_3_669()) { jj_scanpos = xsp; break; }
    }
    return false;
  }

 inline bool jj_3_668()
 {
    if (jj_done) return true;
    if (jj_scan_token(MINUS)) return true;
    if (jj_3R_term_1904_5_270()) return true;
    return false;
  }

 inline bool jj_3_667()
 {
    if (jj_done) return true;
    if (jj_scan_token(PLUS)) return true;
    if (jj_3R_term_1904_5_270()) return true;
    return false;
  }

 inline bool jj_3_666()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_667()) {
    jj_scanpos = xsp;
    if (jj_3_668()) return true;
    }
    return false;
  }

 inline bool jj_3_2178()
 {
    if (jj_done) return true;
    if (jj_3R_set_quantifier_4159_5_396()) return true;
    return false;
  }

 inline bool jj_3R_try_cast_8039_5_250()
 {
    if (jj_done) return true;
    if (jj_scan_token(TRY_CAST)) return true;
    if (jj_scan_token(lparen)) return true;
    if (jj_3R_cast_operand_1737_5_249()) return true;
    if (jj_scan_token(AS)) return true;
    if (jj_3R_cast_target_1744_5_1043()) return true;
    if (jj_scan_token(rparen)) return true;
    return false;
  }

 inline bool jj_3_2180()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_2178()) jj_scanpos = xsp;
    if (jj_3R_value_expression_1855_5_178()) return true;
    while (true) {
      xsp = jj_scanpos;
      if (jj_3_2179()) { jj_scanpos = xsp; break; }
    }
    return false;
  }

 inline bool jj_3R_numeric_value_expression_1894_5_261()
 {
    if (jj_done) return true;
    if (jj_3R_term_1904_5_270()) return true;
    Token * xsp;
    while (true) {
      xsp = jj_scanpos;
      if (jj_3_666()) { jj_scanpos = xsp; break; }
    }
    return false;
  }

 inline bool jj_3_2167()
 {
    if (jj_done) return true;
    if (jj_3R_value_expression_1855_5_178()) return true;
    return false;
  }

 inline bool jj_3_2169()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_2167()) {
    jj_scanpos = xsp;
    if (jj_3_2168()) return true;
    }
    return false;
  }

 inline bool jj_3R_presto_aggregations_8032_5_490()
 {
    if (jj_done) return true;
    if (jj_3R_presto_aggregation_function_8021_5_949()) return true;
    if (jj_scan_token(lparen)) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_2180()) jj_scanpos = xsp;
    if (jj_scan_token(rparen)) return true;
    return false;
  }

 inline bool jj_3_665()
 {
    if (jj_done) return true;
    if (jj_3R_multiset_value_expression_2612_5_269()) return true;
    return false;
  }

 inline bool jj_3R_collection_value_expression_1887_5_267()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_664()) {
    jj_scanpos = xsp;
    if (jj_3_665()) return true;
    }
    return false;
  }

 inline bool jj_3_664()
 {
    if (jj_done) return true;
    if (jj_3R_array_value_expression_2548_5_268()) return true;
    return false;
  }

 inline bool jj_3_2177()
 {
    if (jj_done) return true;
    if (jj_scan_token(344)) return true;
    return false;
  }

 inline bool jj_3_2176()
 {
    if (jj_done) return true;
    if (jj_scan_token(343)) return true;
    return false;
  }

 inline bool jj_3R_reference_value_expression_1881_5_266()
 {
    if (jj_done) return true;
    if (jj_3R_value_expression_primary_1315_5_349()) return true;
    return false;
  }

 inline bool jj_3_2175()
 {
    if (jj_done) return true;
    if (jj_scan_token(342)) return true;
    return false;
  }

 inline bool jj_3_2174()
 {
    if (jj_done) return true;
    if (jj_scan_token(341)) return true;
    return false;
  }

 inline bool jj_3_2173()
 {
    if (jj_done) return true;
    if (jj_scan_token(340)) return true;
    return false;
  }

 inline bool jj_3R_presto_aggregation_function_8021_5_949()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_2172()) {
    jj_scanpos = xsp;
    if (jj_3_2173()) {
    jj_scanpos = xsp;
    if (jj_3_2174()) {
    jj_scanpos = xsp;
    if (jj_3_2175()) {
    jj_scanpos = xsp;
    if (jj_3_2176()) {
    jj_scanpos = xsp;
    if (jj_3_2177()) return true;
    }
    }
    }
    }
    }
    return false;
  }

 inline bool jj_3_2172()
 {
    if (jj_done) return true;
    if (jj_scan_token(338)) return true;
    return false;
  }

 inline bool jj_3R_user_defined_type_value_expression_1875_5_265()
 {
    if (jj_done) return true;
    if (jj_3R_value_expression_primary_1315_5_349()) return true;
    return false;
  }

 inline bool jj_3R_column_description_8015_5_554()
 {
    if (jj_done) return true;
    if (jj_scan_token(COMMENT)) return true;
    if (jj_3R_character_string_literal_866_3_128()) return true;
    return false;
  }

 inline bool jj_3_2166()
 {
    if (jj_done) return true;
    if (jj_3R_set_quantifier_4159_5_396()) return true;
    return false;
  }

 inline bool jj_3_663()
 {
    if (jj_done) return true;
    if (jj_3R_collection_value_expression_1887_5_267()) return true;
    return false;
  }

 inline bool jj_3_662()
 {
    if (jj_done) return true;
    if (jj_3R_reference_value_expression_1881_5_266()) return true;
    return false;
  }

 inline bool jj_3_661()
 {
    if (jj_done) return true;
    if (jj_3R_user_defined_type_value_expression_1875_5_265()) return true;
    return false;
  }

 inline bool jj_3R_routine_description_8009_5_654()
 {
    if (jj_done) return true;
    if (jj_scan_token(COMMENT)) return true;
    if (jj_3R_character_string_literal_866_3_128()) return true;
    return false;
  }

 inline bool jj_3_660()
 {
    if (jj_done) return true;
    if (jj_3R_interval_value_expression_2449_5_264()) return true;
    return false;
  }

 inline bool jj_3_659()
 {
    if (jj_done) return true;
    if (jj_3R_datetime_value_expression_2368_5_263()) return true;
    return false;
  }

 inline bool jj_3_658()
 {
    if (jj_done) return true;
    if (jj_3R_string_value_expression_2121_5_262()) return true;
    return false;
  }

 inline bool jj_3R_common_value_expression_1863_5_259()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_657()) {
    jj_scanpos = xsp;
    if (jj_3_658()) {
    jj_scanpos = xsp;
    if (jj_3_659()) {
    jj_scanpos = xsp;
    if (jj_3_660()) {
    jj_scanpos = xsp;
    if (jj_3_661()) {
    jj_scanpos = xsp;
    if (jj_3_662()) {
    jj_scanpos = xsp;
    if (jj_3_663()) return true;
    }
    }
    }
    }
    }
    }
    return false;
  }

 inline bool jj_3_657()
 {
    if (jj_done) return true;
    if (jj_3R_numeric_value_expression_1894_5_261()) return true;
    return false;
  }

 inline bool jj_3R_table_description_8003_5_521()
 {
    if (jj_done) return true;
    if (jj_scan_token(COMMENT)) return true;
    if (jj_3R_character_string_literal_866_3_128()) return true;
    return false;
  }

 inline bool jj_3_656()
 {
    if (jj_done) return true;
    if (jj_3R_row_value_expression_2737_5_260()) return true;
    return false;
  }

 inline bool jj_3_655()
 {
    if (jj_done) return true;
    if (jj_3R_common_value_expression_1863_5_259()) return true;
    return false;
  }

 inline bool jj_3_2158()
 {
    if (jj_done) return true;
    if (jj_scan_token(570)) return true;
    if (jj_3R_data_type_1086_3_251()) return true;
    return false;
  }

 inline bool jj_3_654()
 {
    if (jj_done) return true;
    if (jj_3R_boolean_value_expression_2494_5_258()) return true;
    return false;
  }

 inline bool jj_3R_value_expression_1855_5_178()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_654()) {
    jj_scanpos = xsp;
    if (jj_3_655()) {
    jj_scanpos = xsp;
    if (jj_3_656()) return true;
    }
    }
    return false;
  }

 inline bool jj_3_2171()
 {
    if (jj_done) return true;
    if (jj_scan_token(COUNT_QUOTED)) return true;
    if (jj_scan_token(lparen)) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_2166()) jj_scanpos = xsp;
    xsp = jj_scanpos;
    if (jj_3_2169()) jj_scanpos = xsp;
    if (jj_scan_token(rparen)) return true;
    return false;
  }

 inline bool jj_3R_count_7996_5_485()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_2170()) {
    jj_scanpos = xsp;
    if (jj_3_2171()) return true;
    }
    return false;
  }

 inline bool jj_3_2170()
 {
    if (jj_done) return true;
    if (jj_scan_token(COUNT)) return true;
    if (jj_scan_token(lparen)) return true;
    if (jj_scan_token(rparen)) return true;
    return false;
  }

 inline bool jj_3R_multiset_element_reference_1849_5_188()
 {
    if (jj_done) return true;
    if (jj_scan_token(ELEMENT)) return true;
    if (jj_scan_token(lparen)) return true;
    if (jj_3R_multiset_value_expression_2612_5_269()) return true;
    if (jj_scan_token(rparen)) return true;
    return false;
  }

 inline bool jj_3R_grouping_expression_7990_5_405()
 {
    if (jj_done) return true;
    if (jj_3R_value_expression_1855_5_178()) return true;
    return false;
  }

 inline bool jj_3R_array_element_reference_1842_5_199()
 {
    if (jj_done) return true;
    if (jj_3R_left_bracket_or_trigraph_804_5_174()) return true;
    if (jj_3R_value_expression_1855_5_178()) return true;
    if (jj_3R_right_bracket_or_trigraph_811_5_175()) return true;
    return false;
  }

 inline bool jj_3_2157()
 {
    if (jj_done) return true;
    if (jj_scan_token(ALL)) return true;
    return false;
  }

 inline bool jj_3R_reference_resolution_1836_5_186()
 {
    if (jj_done) return true;
    if (jj_scan_token(DEREF)) return true;
    if (jj_scan_token(lparen)) return true;
    if (jj_3R_reference_value_expression_1881_5_266()) return true;
    if (jj_scan_token(rparen)) return true;
    return false;
  }

 inline bool jj_3_2148()
 {
    if (jj_done) return true;
    if (jj_scan_token(570)) return true;
    if (jj_3R_actual_identifier_939_5_137()) return true;
    return false;
  }

 inline bool jj_3_650()
 {
    if (jj_done) return true;
    if (jj_3R_SQL_argument_list_3978_6_256()) return true;
    return false;
  }

 inline bool jj_3_2165()
 {
    if (jj_done) return true;
    if (jj_scan_token(MAP)) return true;
    if (jj_scan_token(lparen)) return true;
    if (jj_3R_data_type_1086_3_251()) return true;
    if (jj_scan_token(570)) return true;
    if (jj_3R_data_type_1086_3_251()) return true;
    if (jj_scan_token(rparen)) return true;
    return false;
  }

 inline bool jj_3R_presto_map_type_7971_5_889()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_2164()) {
    jj_scanpos = xsp;
    if (jj_3_2165()) return true;
    }
    return false;
  }

 inline bool jj_3_2164()
 {
    if (jj_done) return true;
    if (jj_scan_token(MAP)) return true;
    if (jj_scan_token(LESS_THAN)) return true;
    if (jj_3R_data_type_1086_3_251()) return true;
    if (jj_scan_token(570)) return true;
    if (jj_3R_data_type_1086_3_251()) return true;
    if (jj_scan_token(GREATER_THAN)) return true;
    return false;
  }

 inline bool jj_3_2163()
 {
    if (jj_done) return true;
    if (jj_scan_token(ARRAY)) return true;
    if (jj_scan_token(lparen)) return true;
    if (jj_3R_data_type_1086_3_251()) return true;
    if (jj_scan_token(rparen)) return true;
    return false;
  }

 inline bool jj_3R_presto_array_type_7964_5_888()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_2162()) {
    jj_scanpos = xsp;
    if (jj_3_2163()) return true;
    }
    return false;
  }

 inline bool jj_3_647()
 {
    if (jj_done) return true;
    if (jj_3R_udaf_filter_8063_4_253()) return true;
    return false;
  }

 inline bool jj_3R_attribute_or_method_reference_1821_5_196()
 {
    if (jj_done) return true;
    if (jj_3R_lambda_body_7925_5_893()) return true;
    return false;
  }

 inline bool jj_3_2162()
 {
    if (jj_done) return true;
    if (jj_scan_token(ARRAY)) return true;
    if (jj_scan_token(LESS_THAN)) return true;
    if (jj_3R_data_type_1086_3_251()) return true;
    if (jj_scan_token(GREATER_THAN)) return true;
    return false;
  }

 inline bool jj_3_2154()
 {
    if (jj_done) return true;
    if (jj_3R_actual_identifier_939_5_137()) return true;
    return false;
  }

 inline bool jj_3_2156()
 {
    if (jj_done) return true;
    if (jj_scan_token(unsigned_integer)) return true;
    return false;
  }

 inline bool jj_3_2161()
 {
    if (jj_done) return true;
    if (jj_scan_token(regular_identifier)) return true;
    if (jj_scan_token(lparen)) return true;
    if (jj_3R_data_type_1086_3_251()) return true;
    Token * xsp;
    while (true) {
      xsp = jj_scanpos;
      if (jj_3_2158()) { jj_scanpos = xsp; break; }
    }
    if (jj_scan_token(rparen)) return true;
    return false;
  }

 inline bool jj_3_653()
 {
    if (jj_done) return true;
    if (jj_3R_routine_invocation_3966_5_257()) return true;
    return false;
  }

 inline bool jj_3_652()
 {
    if (jj_done) return true;
    if (jj_3R_method_invocation_1777_4_197()) return true;
    return false;
  }

 inline bool jj_3_2160()
 {
    if (jj_done) return true;
    if (jj_3R_presto_map_type_7971_5_889()) return true;
    return false;
  }

 inline bool jj_3_2159()
 {
    if (jj_done) return true;
    if (jj_3R_presto_array_type_7964_5_888()) return true;
    return false;
  }

 inline bool jj_3R_presto_generic_type_7956_5_150()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_2159()) {
    jj_scanpos = xsp;
    if (jj_3_2160()) {
    jj_scanpos = xsp;
    if (jj_3_2161()) return true;
    }
    }
    return false;
  }

 inline bool jj_3_2153()
 {
    if (jj_done) return true;
    if (jj_scan_token(568)) return true;
    return false;
  }

 inline bool jj_3R_new_specification_1808_5_185()
 {
    if (jj_done) return true;
    if (jj_scan_token(NEW)) return true;
    if (jj_3R_path_resolved_user_defined_type_name_1281_5_151()) return true;
    if (jj_3R_SQL_argument_list_3978_6_256()) return true;
    return false;
  }

 inline bool jj_3R_limit_clause_7950_5_429()
 {
    if (jj_done) return true;
    if (jj_scan_token(LIMIT)) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_2156()) {
    jj_scanpos = xsp;
    if (jj_3_2157()) return true;
    }
    return false;
  }

 inline bool jj_3_2152()
 {
    if (jj_done) return true;
    if (jj_scan_token(585)) return true;
    return false;
  }

 inline bool jj_3_651()
 {
    if (jj_done) return true;
    if (jj_3R_SQL_argument_list_3978_6_256()) return true;
    return false;
  }

 inline bool jj_3_2155()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_2152()) {
    jj_scanpos = xsp;
    if (jj_3_2153()) return true;
    }
    xsp = jj_scanpos;
    if (jj_3_2154()) jj_scanpos = xsp;
    return false;
  }

 inline bool jj_3R_identifier_suffix_chain_7944_5_138()
 {
    if (jj_done) return true;
    Token * xsp;
    if (jj_3_2155()) return true;
    while (true) {
      xsp = jj_scanpos;
      if (jj_3_2155()) { jj_scanpos = xsp; break; }
    }
    return false;
  }

 inline bool jj_3R_static_method_invocation_1800_5_200()
 {
    if (jj_done) return true;
    if (jj_scan_token(572)) return true;
    if (jj_3R_identifier_928_3_141()) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_651()) jj_scanpos = xsp;
    return false;
  }

 inline bool jj_3_2149()
 {
    if (jj_done) return true;
    if (jj_3R_actual_identifier_939_5_137()) return true;
    Token * xsp;
    while (true) {
      xsp = jj_scanpos;
      if (jj_3_2148()) { jj_scanpos = xsp; break; }
    }
    return false;
  }

 inline bool jj_3R_if_not_exists_7938_5_499()
 {
    if (jj_done) return true;
    if (jj_scan_token(IF)) return true;
    if (jj_scan_token(NOT)) return true;
    if (jj_scan_token(EXISTS)) return true;
    return false;
  }

 inline bool jj_3R_generalized_invocation_1792_6_255()
 {
    if (jj_done) return true;
    if (jj_scan_token(569)) return true;
    if (jj_3R_identifier_928_3_141()) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_650()) jj_scanpos = xsp;
    return false;
  }

 inline bool jj_3_2151()
 {
    if (jj_done) return true;
    if (jj_scan_token(lparen)) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_2149()) jj_scanpos = xsp;
    if (jj_scan_token(rparen)) return true;
    return false;
  }

 inline bool jj_3R_lambda_params_7931_5_473()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_2150()) {
    jj_scanpos = xsp;
    if (jj_3_2151()) return true;
    }
    return false;
  }

 inline bool jj_3_2150()
 {
    if (jj_done) return true;
    if (jj_3R_actual_identifier_939_5_137()) return true;
    return false;
  }

 inline bool jj_3R_direct_invocation_1786_5_254()
 {
    if (jj_done) return true;
    if (jj_3R_SQL_argument_list_3978_6_256()) return true;
    return false;
  }

 inline bool jj_3R_lambda_body_7925_5_893()
 {
    if (jj_done) return true;
    if (jj_scan_token(573)) return true;
    if (jj_3R_value_expression_1855_5_178()) return true;
    return false;
  }

 inline bool jj_3_2144()
 {
    if (jj_done) return true;
    if (jj_3R_all_qualifier_7907_5_887()) return true;
    return false;
  }

 inline bool jj_3_649()
 {
    if (jj_done) return true;
    if (jj_3R_generalized_invocation_1792_6_255()) return true;
    return false;
  }

 inline bool jj_3_2145()
 {
    if (jj_done) return true;
    if (jj_3R_simple_value_specification_1423_5_220()) return true;
    return false;
  }

 inline bool jj_3R_lambda_7919_5_1027()
 {
    if (jj_done) return true;
    if (jj_3R_lambda_params_7931_5_473()) return true;
    if (jj_3R_lambda_body_7925_5_893()) return true;
    return false;
  }

 inline bool jj_3_648()
 {
    if (jj_done) return true;
    if (jj_3R_direct_invocation_1786_5_254()) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_647()) jj_scanpos = xsp;
    return false;
  }

 inline bool jj_3R_method_invocation_1777_4_197()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_648()) {
    jj_scanpos = xsp;
    if (jj_3_649()) return true;
    }
    return false;
  }

 inline bool jj_3_646()
 {
    if (jj_done) return true;
    if (jj_3R_reference_type_1263_5_149()) return true;
    return false;
  }

 inline bool jj_3R_target_subtype_1770_5_1042()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_645()) {
    jj_scanpos = xsp;
    if (jj_3_646()) return true;
    }
    return false;
  }

 inline bool jj_3R_use_statement_7913_5_880()
 {
    if (jj_done) return true;
    if (jj_scan_token(USE)) return true;
    if (jj_3R_identifier_chain_1496_5_207()) return true;
    return false;
  }

 inline bool jj_3_645()
 {
    if (jj_done) return true;
    if (jj_3R_path_resolved_user_defined_type_name_1281_5_151()) return true;
    return false;
  }

 inline bool jj_3_2147()
 {
    if (jj_done) return true;
    if (jj_scan_token(CONDITION)) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_2145()) jj_scanpos = xsp;
    return false;
  }

 inline bool jj_3R_all_qualifier_7907_5_887()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_2146()) {
    jj_scanpos = xsp;
    if (jj_3_2147()) return true;
    }
    return false;
  }

 inline bool jj_3R_subtype_treatment_1764_5_184()
 {
    if (jj_done) return true;
    if (jj_scan_token(TREAT)) return true;
    if (jj_scan_token(lparen)) return true;
    if (jj_3R_value_expression_1855_5_178()) return true;
    if (jj_scan_token(AS)) return true;
    if (jj_3R_target_subtype_1770_5_1042()) return true;
    if (jj_scan_token(rparen)) return true;
    return false;
  }

 inline bool jj_3_2146()
 {
    if (jj_done) return true;
    if (jj_scan_token(STATEMENT)) return true;
    return false;
  }

 inline bool jj_3R_all_info_target_7901_5_1021()
 {
    if (jj_done) return true;
    if (jj_3R_simple_target_specification_1452_5_1022()) return true;
    return false;
  }

 inline bool jj_3R_field_reference_1757_5_195()
 {
    if (jj_done) return true;
    if (jj_scan_token(569)) return true;
    if (jj_3R_identifier_928_3_141()) return true;
    return false;
  }

 inline bool jj_3R_all_information_7895_5_884()
 {
    if (jj_done) return true;
    if (jj_3R_all_info_target_7901_5_1021()) return true;
    if (jj_scan_token(EQUAL)) return true;
    if (jj_scan_token(ALL)) return true;
    return false;
  }

 inline bool jj_3R_next_value_expression_1751_5_189()
 {
    if (jj_done) return true;
    if (jj_scan_token(NEXT)) return true;
    if (jj_scan_token(VALUE)) return true;
    if (jj_scan_token(FOR)) return true;
    if (jj_3R_schema_qualified_name_970_5_252()) return true;
    return false;
  }

 inline bool jj_3_2143()
 {
    if (jj_done) return true;
    if (jj_scan_token(TRIGGER_SCHEMA)) return true;
    return false;
  }

 inline bool jj_3_644()
 {
    if (jj_done) return true;
    if (jj_3R_schema_qualified_name_970_5_252()) return true;
    return false;
  }

 inline bool jj_3_2142()
 {
    if (jj_done) return true;
    if (jj_scan_token(TRIGGER_NAME)) return true;
    return false;
  }

 inline bool jj_3R_cast_target_1744_5_1043()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_643()) {
    jj_scanpos = xsp;
    if (jj_3_644()) return true;
    }
    return false;
  }

 inline bool jj_3_643()
 {
    if (jj_done) return true;
    if (jj_3R_data_type_1086_3_251()) return true;
    return false;
  }

 inline bool jj_3_2141()
 {
    if (jj_done) return true;
    if (jj_scan_token(TRIGGER_CATALOG)) return true;
    return false;
  }

 inline bool jj_3_2140()
 {
    if (jj_done) return true;
    if (jj_scan_token(TABLE_NAME)) return true;
    return false;
  }

 inline bool jj_3_2139()
 {
    if (jj_done) return true;
    if (jj_scan_token(SUBCLASS_ORIGIN)) return true;
    return false;
  }

 inline bool jj_3_2138()
 {
    if (jj_done) return true;
    if (jj_scan_token(SPECIFIC_NAME)) return true;
    return false;
  }

 inline bool jj_3_2137()
 {
    if (jj_done) return true;
    if (jj_scan_token(SERVER_NAME)) return true;
    return false;
  }

 inline bool jj_3_2136()
 {
    if (jj_done) return true;
    if (jj_scan_token(SCHEMA_NAME)) return true;
    return false;
  }

 inline bool jj_3_642()
 {
    if (jj_done) return true;
    if (jj_3R_implicitly_typed_value_specification_1482_5_209()) return true;
    return false;
  }

 inline bool jj_3_2135()
 {
    if (jj_done) return true;
    if (jj_scan_token(ROUTINE_SCHEMA)) return true;
    return false;
  }

 inline bool jj_3R_cast_operand_1737_5_249()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_641()) {
    jj_scanpos = xsp;
    if (jj_3_642()) return true;
    }
    return false;
  }

 inline bool jj_3_641()
 {
    if (jj_done) return true;
    if (jj_3R_value_expression_1855_5_178()) return true;
    return false;
  }

 inline bool jj_3_2134()
 {
    if (jj_done) return true;
    if (jj_scan_token(ROUTINE_NAME)) return true;
    return false;
  }

 inline bool jj_3_2133()
 {
    if (jj_done) return true;
    if (jj_scan_token(ROUTINE_CATALOG)) return true;
    return false;
  }

 inline bool jj_3_2132()
 {
    if (jj_done) return true;
    if (jj_scan_token(RETURNED_SQLSTATE)) return true;
    return false;
  }

 inline bool jj_3_2131()
 {
    if (jj_done) return true;
    if (jj_scan_token(PARAMETER_ORDINAL_POSITION)) return true;
    return false;
  }

 inline bool jj_3_2130()
 {
    if (jj_done) return true;
    if (jj_scan_token(PARAMETER_NAME)) return true;
    return false;
  }

 inline bool jj_3_2129()
 {
    if (jj_done) return true;
    if (jj_scan_token(PARAMETER_MODE)) return true;
    return false;
  }

 inline bool jj_3_640()
 {
    if (jj_done) return true;
    if (jj_3R_try_cast_8039_5_250()) return true;
    return false;
  }

 inline bool jj_3_2128()
 {
    if (jj_done) return true;
    if (jj_scan_token(MESSAGE_TEXT)) return true;
    return false;
  }

 inline bool jj_3_639()
 {
    if (jj_done) return true;
    if (jj_scan_token(CAST)) return true;
    if (jj_scan_token(lparen)) return true;
    if (jj_3R_cast_operand_1737_5_249()) return true;
    if (jj_scan_token(AS)) return true;
    if (jj_3R_cast_target_1744_5_1043()) return true;
    if (jj_scan_token(rparen)) return true;
    return false;
  }

 inline bool jj_3R_cast_specification_1730_5_183()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_639()) {
    jj_scanpos = xsp;
    if (jj_3_640()) return true;
    }
    return false;
  }

 inline bool jj_3_2127()
 {
    if (jj_done) return true;
    if (jj_scan_token(MESSAGE_OCTET_LENGTH)) return true;
    return false;
  }

 inline bool jj_3_2126()
 {
    if (jj_done) return true;
    if (jj_scan_token(MESSAGE_LENGTH)) return true;
    return false;
  }

 inline bool jj_3_2125()
 {
    if (jj_done) return true;
    if (jj_scan_token(CURSOR_NAME)) return true;
    return false;
  }

 inline bool jj_3_2124()
 {
    if (jj_done) return true;
    if (jj_scan_token(CONSTRAINT_SCHEMA)) return true;
    return false;
  }

 inline bool jj_3_2123()
 {
    if (jj_done) return true;
    if (jj_scan_token(CONSTRAINT_NAME)) return true;
    return false;
  }

 inline bool jj_3_638()
 {
    if (jj_done) return true;
    if (jj_scan_token(NULL_)) return true;
    return false;
  }

 inline bool jj_3_2122()
 {
    if (jj_done) return true;
    if (jj_scan_token(CONSTRAINT_CATALOG)) return true;
    return false;
  }

 inline bool jj_3_2121()
 {
    if (jj_done) return true;
    if (jj_scan_token(CONNECTION_NAME)) return true;
    return false;
  }

 inline bool jj_3R_result_1723_5_901()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_637()) {
    jj_scanpos = xsp;
    if (jj_3_638()) return true;
    }
    return false;
  }

 inline bool jj_3_637()
 {
    if (jj_done) return true;
    if (jj_3R_value_expression_1855_5_178()) return true;
    return false;
  }

 inline bool jj_3_2120()
 {
    if (jj_done) return true;
    if (jj_scan_token(CONDITION_NUMBER)) return true;
    return false;
  }

 inline bool jj_3_2119()
 {
    if (jj_done) return true;
    if (jj_scan_token(COLUMN_NAME)) return true;
    return false;
  }

 inline bool jj_3_2118()
 {
    if (jj_done) return true;
    if (jj_scan_token(CLASS_ORIGIN)) return true;
    return false;
  }

 inline bool jj_3_2117()
 {
    if (jj_done) return true;
    if (jj_scan_token(CATALOG_NAME)) return true;
    return false;
  }

 inline bool jj_3_636()
 {
    if (jj_done) return true;
    if (jj_3R_type_predicate_part_2_3836_5_248()) return true;
    return false;
  }

 inline bool jj_3_635()
 {
    if (jj_done) return true;
    if (jj_3R_set_predicate_part_2_3824_5_247()) return true;
    return false;
  }

 inline bool jj_3R_condition_information_item_7857_5_886()
 {
    if (jj_done) return true;
    if (jj_3R_simple_target_specification_1452_5_1022()) return true;
    if (jj_scan_token(EQUAL)) return true;
    return false;
  }

 inline bool jj_3_634()
 {
    if (jj_done) return true;
    if (jj_3R_submultiset_predicate_part_2_3812_5_246()) return true;
    return false;
  }

 inline bool jj_3_633()
 {
    if (jj_done) return true;
    if (jj_3R_member_predicate_part_2_3800_5_245()) return true;
    return false;
  }

 inline bool jj_3_632()
 {
    if (jj_done) return true;
    if (jj_3R_distinct_predicate_part_2_3776_5_244()) return true;
    return false;
  }

 inline bool jj_3_631()
 {
    if (jj_done) return true;
    if (jj_3R_overlaps_predicate_part_2_3752_5_243()) return true;
    return false;
  }

 inline bool jj_3_2105()
 {
    if (jj_done) return true;
    if (jj_scan_token(570)) return true;
    if (jj_3R_statement_information_item_7829_5_885()) return true;
    return false;
  }

 inline bool jj_3_2116()
 {
    if (jj_done) return true;
    if (jj_scan_token(570)) return true;
    if (jj_3R_condition_information_item_7857_5_886()) return true;
    return false;
  }

 inline bool jj_3_630()
 {
    if (jj_done) return true;
    if (jj_3R_match_predicate_part_2_3734_5_242()) return true;
    return false;
  }

 inline bool jj_3_629()
 {
    if (jj_done) return true;
    if (jj_3R_normalized_predicate_part_2_3722_5_241()) return true;
    return false;
  }

 inline bool jj_3_628()
 {
    if (jj_done) return true;
    if (jj_3R_quantified_comparison_predicate_part_2_3698_5_240()) return true;
    return false;
  }

 inline bool jj_3R_condition_information_7850_5_883()
 {
    if (jj_done) return true;
    if (jj_scan_token(CONDITION)) return true;
    if (jj_3R_simple_value_specification_1423_5_220()) return true;
    if (jj_3R_condition_information_item_7857_5_886()) return true;
    return false;
  }

 inline bool jj_3_627()
 {
    if (jj_done) return true;
    if (jj_3R_null_predicate_part_2_3686_5_239()) return true;
    return false;
  }

 inline bool jj_3_618()
 {
    if (jj_done) return true;
    if (jj_scan_token(570)) return true;
    if (jj_3R_when_operand_1696_5_231()) return true;
    return false;
  }

 inline bool jj_3_626()
 {
    if (jj_done) return true;
    if (jj_3R_regex_like_predicate_part_2_3674_5_238()) return true;
    return false;
  }

 inline bool jj_3_625()
 {
    if (jj_done) return true;
    if (jj_3R_similar_predicate_part_2_3662_5_237()) return true;
    return false;
  }

 inline bool jj_3_624()
 {
    if (jj_done) return true;
    if (jj_3R_octet_like_predicate_part_2_3650_5_236()) return true;
    return false;
  }

 inline bool jj_3_623()
 {
    if (jj_done) return true;
    if (jj_3R_character_like_predicate_part_2_3638_5_235()) return true;
    return false;
  }

 inline bool jj_3_613()
 {
    if (jj_done) return true;
    if (jj_3R_else_clause_1677_5_227()) return true;
    return false;
  }

 inline bool jj_3_622()
 {
    if (jj_done) return true;
    if (jj_3R_in_predicate_part_2_3606_5_234()) return true;
    return false;
  }

 inline bool jj_3_621()
 {
    if (jj_done) return true;
    if (jj_3R_between_predicate_part_2_3593_5_233()) return true;
    return false;
  }

 inline bool jj_3_2115()
 {
    if (jj_done) return true;
    if (jj_scan_token(TRANSACTION_ACTIVE)) return true;
    return false;
  }

 inline bool jj_3_620()
 {
    if (jj_done) return true;
    if (jj_3R_comparison_predicate_part_2_3569_5_232()) return true;
    return false;
  }

 inline bool jj_3_2114()
 {
    if (jj_done) return true;
    if (jj_scan_token(TRANSACTIONS_ROLLED_BACK)) return true;
    return false;
  }

 inline bool jj_3_619()
 {
    if (jj_done) return true;
    if (jj_3R_row_value_predicand_2758_5_229()) return true;
    return false;
  }

 inline bool jj_3_2113()
 {
    if (jj_done) return true;
    if (jj_scan_token(TRANSACTIONS_COMMITTED)) return true;
    return false;
  }

 inline bool jj_3_2112()
 {
    if (jj_done) return true;
    if (jj_scan_token(ROW_COUNT)) return true;
    return false;
  }

 inline bool jj_3_2111()
 {
    if (jj_done) return true;
    if (jj_scan_token(DYNAMIC_FUNCTION_CODE)) return true;
    return false;
  }

 inline bool jj_3_615()
 {
    if (jj_done) return true;
    if (jj_3R_else_clause_1677_5_227()) return true;
    return false;
  }

 inline bool jj_3R_when_operand_1696_5_231()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_619()) {
    jj_scanpos = xsp;
    if (jj_3_620()) {
    jj_scanpos = xsp;
    if (jj_3_621()) {
    jj_scanpos = xsp;
    if (jj_3_622()) {
    jj_scanpos = xsp;
    if (jj_3_623()) {
    jj_scanpos = xsp;
    if (jj_3_624()) {
    jj_scanpos = xsp;
    if (jj_3_625()) {
    jj_scanpos = xsp;
    if (jj_3_626()) {
    jj_scanpos = xsp;
    if (jj_3_627()) {
    jj_scanpos = xsp;
    if (jj_3_628()) {
    jj_scanpos = xsp;
    if (jj_3_629()) {
    jj_scanpos = xsp;
    if (jj_3_630()) {
    jj_scanpos = xsp;
    if (jj_3_631()) {
    jj_scanpos = xsp;
    if (jj_3_632()) {
    jj_scanpos = xsp;
    if (jj_3_633()) {
    jj_scanpos = xsp;
    if (jj_3_634()) {
    jj_scanpos = xsp;
    if (jj_3_635()) {
    jj_scanpos = xsp;
    if (jj_3_636()) return true;
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    return false;
  }

 inline bool jj_3_2110()
 {
    if (jj_done) return true;
    if (jj_scan_token(DYNAMIC_FUNCTION)) return true;
    return false;
  }

 inline bool jj_3_2109()
 {
    if (jj_done) return true;
    if (jj_scan_token(COMMAND_FUNCTION_CODE)) return true;
    return false;
  }

 inline bool jj_3_2108()
 {
    if (jj_done) return true;
    if (jj_scan_token(COMMAND_FUNCTION)) return true;
    return false;
  }

 inline bool jj_3_2107()
 {
    if (jj_done) return true;
    if (jj_scan_token(MORE_)) return true;
    return false;
  }

 inline bool jj_3R_statement_information_item_name_7835_5_1036()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_2106()) {
    jj_scanpos = xsp;
    if (jj_3_2107()) {
    jj_scanpos = xsp;
    if (jj_3_2108()) {
    jj_scanpos = xsp;
    if (jj_3_2109()) {
    jj_scanpos = xsp;
    if (jj_3_2110()) {
    jj_scanpos = xsp;
    if (jj_3_2111()) {
    jj_scanpos = xsp;
    if (jj_3_2112()) {
    jj_scanpos = xsp;
    if (jj_3_2113()) {
    jj_scanpos = xsp;
    if (jj_3_2114()) {
    jj_scanpos = xsp;
    if (jj_3_2115()) return true;
    }
    }
    }
    }
    }
    }
    }
    }
    }
    return false;
  }

 inline bool jj_3_2106()
 {
    if (jj_done) return true;
    if (jj_scan_token(NUMBER)) return true;
    return false;
  }

 inline bool jj_3R_when_operand_list_1690_5_900()
 {
    if (jj_done) return true;
    if (jj_3R_when_operand_1696_5_231()) return true;
    Token * xsp;
    while (true) {
      xsp = jj_scanpos;
      if (jj_3_618()) { jj_scanpos = xsp; break; }
    }
    return false;
  }

 inline bool jj_3R_statement_information_item_7829_5_885()
 {
    if (jj_done) return true;
    if (jj_3R_simple_target_specification_1452_5_1022()) return true;
    if (jj_scan_token(EQUAL)) return true;
    if (jj_3R_statement_information_item_name_7835_5_1036()) return true;
    return false;
  }

 inline bool jj_3_617()
 {
    if (jj_done) return true;
    if (jj_3R_overlaps_predicate_part_1_3746_5_230()) return true;
    return false;
  }

 inline bool jj_3R_case_operand_1683_5_899()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_616()) {
    jj_scanpos = xsp;
    if (jj_3_617()) return true;
    }
    return false;
  }

 inline bool jj_3_616()
 {
    if (jj_done) return true;
    if (jj_3R_row_value_predicand_2758_5_229()) return true;
    return false;
  }

 inline bool jj_3R_statement_information_7823_5_882()
 {
    if (jj_done) return true;
    if (jj_3R_statement_information_item_7829_5_885()) return true;
    return false;
  }

 inline bool jj_3_598()
 {
    if (jj_done) return true;
    if (jj_3R_null_treatment_1581_5_218()) return true;
    return false;
  }

 inline bool jj_3_607()
 {
    if (jj_done) return true;
    if (jj_scan_token(570)) return true;
    if (jj_3R_value_expression_1855_5_178()) return true;
    return false;
  }

 inline bool jj_3_612()
 {
    if (jj_done) return true;
    if (jj_3R_simple_when_clause_1665_5_226()) return true;
    return false;
  }

 inline bool jj_3R_else_clause_1677_5_227()
 {
    if (jj_done) return true;
    if (jj_scan_token(ELSE)) return true;
    if (jj_3R_result_1723_5_901()) return true;
    return false;
  }

 inline bool jj_3_2104()
 {
    if (jj_done) return true;
    if (jj_3R_all_information_7895_5_884()) return true;
    return false;
  }

 inline bool jj_3_2103()
 {
    if (jj_done) return true;
    if (jj_3R_condition_information_7850_5_883()) return true;
    return false;
  }

 inline bool jj_3R_SQL_diagnostics_information_7815_5_1034()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_2102()) {
    jj_scanpos = xsp;
    if (jj_3_2103()) {
    jj_scanpos = xsp;
    if (jj_3_2104()) return true;
    }
    }
    return false;
  }

 inline bool jj_3_2102()
 {
    if (jj_done) return true;
    if (jj_3R_statement_information_7823_5_882()) return true;
    return false;
  }

 inline bool jj_3R_searched_when_clause_1671_5_228()
 {
    if (jj_done) return true;
    if (jj_scan_token(WHEN)) return true;
    if (jj_3R_search_condition_3868_5_818()) return true;
    if (jj_scan_token(THEN)) return true;
    if (jj_3R_result_1723_5_901()) return true;
    return false;
  }

 inline bool jj_3_614()
 {
    if (jj_done) return true;
    if (jj_3R_searched_when_clause_1671_5_228()) return true;
    return false;
  }

 inline bool jj_3R_get_diagnostics_statement_7809_5_991()
 {
    if (jj_done) return true;
    if (jj_scan_token(GET)) return true;
    if (jj_scan_token(DIAGNOSTICS)) return true;
    if (jj_3R_SQL_diagnostics_information_7815_5_1034()) return true;
    return false;
  }

 inline bool jj_3R_simple_when_clause_1665_5_226()
 {
    if (jj_done) return true;
    if (jj_scan_token(WHEN)) return true;
    if (jj_3R_when_operand_list_1690_5_900()) return true;
    if (jj_scan_token(THEN)) return true;
    if (jj_3R_result_1723_5_901()) return true;
    return false;
  }

 inline bool jj_3R_direct_select_statement_multiple_rows_7803_5_881()
 {
    if (jj_done) return true;
    if (jj_3R_cursor_specification_6733_5_1018()) return true;
    return false;
  }

 inline bool jj_3R_searched_case_1659_5_225()
 {
    if (jj_done) return true;
    if (jj_scan_token(CASE)) return true;
    Token * xsp;
    if (jj_3_614()) return true;
    while (true) {
      xsp = jj_scanpos;
      if (jj_3_614()) { jj_scanpos = xsp; break; }
    }
    xsp = jj_scanpos;
    if (jj_3_615()) jj_scanpos = xsp;
    if (jj_scan_token(END)) return true;
    return false;
  }

 inline bool jj_3R_direct_implementation_defined_statement_7797_5_879()
 {
    if (jj_done) return true;
    if (jj_3R_character_string_literal_866_3_128()) return true;
    return false;
  }

 inline bool jj_3_597()
 {
    if (jj_done) return true;
    if (jj_3R_from_first_or_last_1612_5_219()) return true;
    return false;
  }

 inline bool jj_3R_simple_case_1653_5_224()
 {
    if (jj_done) return true;
    if (jj_scan_token(CASE)) return true;
    if (jj_3R_case_operand_1683_5_899()) return true;
    Token * xsp;
    if (jj_3_612()) return true;
    while (true) {
      xsp = jj_scanpos;
      if (jj_3_612()) { jj_scanpos = xsp; break; }
    }
    xsp = jj_scanpos;
    if (jj_3_613()) jj_scanpos = xsp;
    if (jj_scan_token(END)) return true;
    return false;
  }

 inline bool jj_3_611()
 {
    if (jj_done) return true;
    if (jj_3R_searched_case_1659_5_225()) return true;
    return false;
  }

 inline bool jj_3_2101()
 {
    if (jj_done) return true;
    if (jj_3R_temporary_table_declaration_7036_5_718()) return true;
    return false;
  }

 inline bool jj_3_610()
 {
    if (jj_done) return true;
    if (jj_3R_simple_case_1653_5_224()) return true;
    return false;
  }

 inline bool jj_3R_case_specification_1646_5_223()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_610()) {
    jj_scanpos = xsp;
    if (jj_3_611()) return true;
    }
    return false;
  }

 inline bool jj_3_2100()
 {
    if (jj_done) return true;
    if (jj_3R_merge_statement_6878_5_385()) return true;
    return false;
  }

 inline bool jj_3_2099()
 {
    if (jj_done) return true;
    if (jj_3R_truncate_table_statement_6810_5_771()) return true;
    return false;
  }

 inline bool jj_3_2098()
 {
    if (jj_done) return true;
    if (jj_3R_update_statement_searched_6963_5_386()) return true;
    return false;
  }

 inline bool jj_3_2097()
 {
    if (jj_done) return true;
    if (jj_3R_insert_statement_6823_5_384()) return true;
    return false;
  }

 inline bool jj_3_2096()
 {
    if (jj_done) return true;
    if (jj_3R_direct_select_statement_multiple_rows_7803_5_881()) return true;
    return false;
  }

 inline bool jj_3R_direct_SQL_data_statement_7784_5_878()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_2095()) {
    jj_scanpos = xsp;
    if (jj_3_2096()) {
    jj_scanpos = xsp;
    if (jj_3_2097()) {
    jj_scanpos = xsp;
    if (jj_3_2098()) {
    jj_scanpos = xsp;
    if (jj_3_2099()) {
    jj_scanpos = xsp;
    if (jj_3_2100()) {
    jj_scanpos = xsp;
    if (jj_3_2101()) return true;
    }
    }
    }
    }
    }
    }
    return false;
  }

 inline bool jj_3_609()
 {
    if (jj_done) return true;
    if (jj_scan_token(COALESCE)) return true;
    if (jj_scan_token(lparen)) return true;
    if (jj_3R_value_expression_1855_5_178()) return true;
    Token * xsp;
    if (jj_3_607()) return true;
    while (true) {
      xsp = jj_scanpos;
      if (jj_3_607()) { jj_scanpos = xsp; break; }
    }
    if (jj_scan_token(rparen)) return true;
    return false;
  }

 inline bool jj_3_2095()
 {
    if (jj_done) return true;
    if (jj_3R_delete_statement_searched_6803_5_383()) return true;
    return false;
  }

 inline bool jj_3_608()
 {
    if (jj_done) return true;
    if (jj_scan_token(NULLIF)) return true;
    if (jj_scan_token(lparen)) return true;
    if (jj_3R_value_expression_1855_5_178()) return true;
    if (jj_scan_token(570)) return true;
    if (jj_3R_value_expression_1855_5_178()) return true;
    if (jj_scan_token(rparen)) return true;
    return false;
  }

 inline bool jj_3R_case_abbreviation_1639_5_222()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_608()) {
    jj_scanpos = xsp;
    if (jj_3_609()) return true;
    }
    return false;
  }

 inline bool jj_3_594()
 {
    if (jj_done) return true;
    if (jj_3R_null_treatment_1581_5_218()) return true;
    return false;
  }

 inline bool jj_3_2094()
 {
    if (jj_done) return true;
    if (jj_3R_use_statement_7913_5_880()) return true;
    return false;
  }

 inline bool jj_3_2093()
 {
    if (jj_done) return true;
    if (jj_3R_direct_implementation_defined_statement_7797_5_879()) return true;
    return false;
  }

 inline bool jj_3_606()
 {
    if (jj_done) return true;
    if (jj_3R_case_specification_1646_5_223()) return true;
    return false;
  }

 inline bool jj_3_2092()
 {
    if (jj_done) return true;
    if (jj_3R_SQL_session_statement_6636_5_734()) return true;
    return false;
  }

 inline bool jj_3_605()
 {
    if (jj_done) return true;
    if (jj_3R_case_abbreviation_1639_5_222()) return true;
    return false;
  }

 inline bool jj_3R_case_expression_1632_5_182()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_605()) {
    jj_scanpos = xsp;
    if (jj_3_606()) return true;
    }
    return false;
  }

 inline bool jj_3_2091()
 {
    if (jj_done) return true;
    if (jj_3R_SQL_connection_statement_6628_5_733()) return true;
    return false;
  }

 inline bool jj_3_2090()
 {
    if (jj_done) return true;
    if (jj_3R_SQL_transaction_statement_6616_5_732()) return true;
    return false;
  }

 inline bool jj_3_2089()
 {
    if (jj_done) return true;
    if (jj_3R_SQL_schema_statement_6526_5_729()) return true;
    return false;
  }

 inline bool jj_3_2088()
 {
    if (jj_done) return true;
    if (jj_3R_direct_SQL_data_statement_7784_5_878()) return true;
    return false;
  }

 inline bool jj_3_2087()
 {
    if (jj_done) return true;
    if (jj_3R_target_table_6796_5_877()) return true;
    return false;
  }

 inline bool jj_3R_in_line_window_specification_1626_5_221()
 {
    if (jj_done) return true;
    if (jj_3R_window_specification_3204_6_898()) return true;
    return false;
  }

 inline bool jj_3_604()
 {
    if (jj_done) return true;
    if (jj_3R_identifier_928_3_141()) return true;
    return false;
  }

 inline bool jj_3R_window_name_or_specification_1619_5_894()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_603()) {
    jj_scanpos = xsp;
    if (jj_3_604()) return true;
    }
    return false;
  }

 inline bool jj_3_603()
 {
    if (jj_done) return true;
    if (jj_3R_in_line_window_specification_1626_5_221()) return true;
    return false;
  }

 inline bool jj_3R_preparable_dynamic_update_statement_positioned_7759_5_860()
 {
    if (jj_done) return true;
    if (jj_scan_token(UPDATE)) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_2087()) jj_scanpos = xsp;
    if (jj_scan_token(SET)) return true;
    if (jj_3R_set_clause_list_6971_5_1010()) return true;
    return false;
  }

 inline bool jj_3_2085()
 {
    if (jj_done) return true;
    if (jj_scan_token(FROM)) return true;
    if (jj_3R_target_table_6796_5_877()) return true;
    return false;
  }

 inline bool jj_3_602()
 {
    if (jj_done) return true;
    if (jj_scan_token(FROM)) return true;
    if (jj_scan_token(LAST)) return true;
    return false;
  }

 inline bool jj_3_601()
 {
    if (jj_done) return true;
    if (jj_scan_token(FROM)) return true;
    if (jj_scan_token(FIRST)) return true;
    return false;
  }

 inline bool jj_3R_from_first_or_last_1612_5_219()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_601()) {
    jj_scanpos = xsp;
    if (jj_3_602()) return true;
    }
    return false;
  }

 inline bool jj_3_2086()
 {
    if (jj_done) return true;
    if (jj_3R_scope_option_1079_5_143()) return true;
    return false;
  }

 inline bool jj_3_596()
 {
    if (jj_done) return true;
    if (jj_scan_token(LAST_VALUE)) return true;
    return false;
  }

 inline bool jj_3_600()
 {
    if (jj_done) return true;
    if (jj_scan_token(571)) return true;
    return false;
  }

 inline bool jj_3R_nth_row_1605_5_1044()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_599()) {
    jj_scanpos = xsp;
    if (jj_3_600()) return true;
    }
    return false;
  }

 inline bool jj_3_599()
 {
    if (jj_done) return true;
    if (jj_3R_simple_value_specification_1423_5_220()) return true;
    return false;
  }

 inline bool jj_3R_preparable_dynamic_delete_statement_positioned_7746_5_859()
 {
    if (jj_done) return true;
    if (jj_scan_token(DELETE)) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_2085()) jj_scanpos = xsp;
    if (jj_scan_token(WHERE)) return true;
    if (jj_scan_token(CURRENT)) return true;
    return false;
  }

 inline bool jj_3_593()
 {
    if (jj_done) return true;
    if (jj_scan_token(IGNORE)) return true;
    if (jj_scan_token(NULLS)) return true;
    return false;
  }

 inline bool jj_3R_nth_value_function_1599_5_217()
 {
    if (jj_done) return true;
    if (jj_scan_token(NTH_VALUE)) return true;
    if (jj_scan_token(lparen)) return true;
    if (jj_3R_value_expression_1855_5_178()) return true;
    if (jj_scan_token(570)) return true;
    if (jj_3R_nth_row_1605_5_1044()) return true;
    if (jj_scan_token(rparen)) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_597()) jj_scanpos = xsp;
    xsp = jj_scanpos;
    if (jj_3_598()) jj_scanpos = xsp;
    return false;
  }

 inline bool jj_3_2082()
 {
    if (jj_done) return true;
    if (jj_3R_input_using_clause_7601_5_876()) return true;
    return false;
  }

 inline bool jj_3R_dynamic_update_statement_positioned_7739_5_806()
 {
    if (jj_done) return true;
    if (jj_scan_token(UPDATE)) return true;
    if (jj_3R_target_table_6796_5_877()) return true;
    if (jj_scan_token(SET)) return true;
    return false;
  }

 inline bool jj_3_587()
 {
    if (jj_done) return true;
    if (jj_scan_token(570)) return true;
    if (jj_3R_value_expression_1855_5_178()) return true;
    return false;
  }

 inline bool jj_3R_first_or_last_value_1593_5_897()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_595()) {
    jj_scanpos = xsp;
    if (jj_3_596()) return true;
    }
    return false;
  }

 inline bool jj_3_595()
 {
    if (jj_done) return true;
    if (jj_scan_token(FIRST_VALUE)) return true;
    return false;
  }

 inline bool jj_3R_dynamic_delete_statement_positioned_7733_5_805()
 {
    if (jj_done) return true;
    if (jj_scan_token(DELETE)) return true;
    if (jj_scan_token(FROM)) return true;
    if (jj_3R_target_table_6796_5_877()) return true;
    return false;
  }

 inline bool jj_3R_first_or_last_value_function_1587_5_216()
 {
    if (jj_done) return true;
    if (jj_3R_first_or_last_value_1593_5_897()) return true;
    if (jj_scan_token(lparen)) return true;
    if (jj_3R_value_expression_1855_5_178()) return true;
    if (jj_scan_token(rparen)) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_594()) jj_scanpos = xsp;
    return false;
  }

 inline bool jj_3R_dynamic_close_statement_7727_5_804()
 {
    if (jj_done) return true;
    if (jj_scan_token(CLOSE)) return true;
    if (jj_3R_dynamic_cursor_name_1053_5_1006()) return true;
    return false;
  }

 inline bool jj_3_591()
 {
    if (jj_done) return true;
    if (jj_scan_token(LAG)) return true;
    return false;
  }

 inline bool jj_3_2083()
 {
    if (jj_done) return true;
    if (jj_3R_fetch_orientation_6757_5_816()) return true;
    return false;
  }

 inline bool jj_3_2084()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_2083()) jj_scanpos = xsp;
    if (jj_scan_token(FROM)) return true;
    return false;
  }

 inline bool jj_3_592()
 {
    if (jj_done) return true;
    if (jj_scan_token(RESPECT)) return true;
    if (jj_scan_token(NULLS)) return true;
    return false;
  }

 inline bool jj_3R_null_treatment_1581_5_218()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_592()) {
    jj_scanpos = xsp;
    if (jj_3_593()) return true;
    }
    return false;
  }

 inline bool jj_3R_dynamic_single_row_select_statement_7721_5_857()
 {
    if (jj_done) return true;
    if (jj_3R_query_specification_3323_5_440()) return true;
    return false;
  }

 inline bool jj_3R_lead_or_lag_1575_5_896()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_590()) {
    jj_scanpos = xsp;
    if (jj_3_591()) return true;
    }
    return false;
  }

 inline bool jj_3_590()
 {
    if (jj_done) return true;
    if (jj_scan_token(LEAD)) return true;
    return false;
  }

 inline bool jj_3R_dynamic_fetch_statement_7715_5_803()
 {
    if (jj_done) return true;
    if (jj_scan_token(FETCH)) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_2084()) jj_scanpos = xsp;
    if (jj_3R_dynamic_cursor_name_1053_5_1006()) return true;
    if (jj_3R_output_using_clause_7626_5_1007()) return true;
    return false;
  }

 inline bool jj_3_589()
 {
    if (jj_done) return true;
    if (jj_3R_null_treatment_1581_5_218()) return true;
    return false;
  }

 inline bool jj_3_588()
 {
    if (jj_done) return true;
    if (jj_scan_token(570)) return true;
    if (jj_3R_value_expression_1855_5_178()) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_587()) jj_scanpos = xsp;
    return false;
  }

 inline bool jj_3R_dynamic_open_statement_7709_5_802()
 {
    if (jj_done) return true;
    if (jj_scan_token(OPEN)) return true;
    if (jj_3R_dynamic_cursor_name_1053_5_1006()) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_2082()) jj_scanpos = xsp;
    return false;
  }

 inline bool jj_3R_lead_or_lag_function_1564_5_215()
 {
    if (jj_done) return true;
    if (jj_3R_lead_or_lag_1575_5_896()) return true;
    if (jj_scan_token(lparen)) return true;
    if (jj_3R_value_expression_1855_5_178()) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_588()) jj_scanpos = xsp;
    if (jj_scan_token(rparen)) return true;
    xsp = jj_scanpos;
    if (jj_3_589()) jj_scanpos = xsp;
    return false;
  }

 inline bool jj_3_2078()
 {
    if (jj_done) return true;
    if (jj_3R_parameter_using_clause_7663_5_873()) return true;
    return false;
  }

 inline bool jj_3_2081()
 {
    if (jj_done) return true;
    if (jj_scan_token(CURSOR)) return true;
    return false;
  }

 inline bool jj_3R_result_set_cursor_7703_5_875()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_2081()) jj_scanpos = xsp;
    if (jj_scan_token(FOR)) return true;
    if (jj_scan_token(PROCEDURE)) return true;
    if (jj_3R_specific_routine_designator_4041_5_708()) return true;
    return false;
  }

 inline bool jj_3_586()
 {
    if (jj_done) return true;
    if (jj_scan_token(571)) return true;
    return false;
  }

 inline bool jj_3R_number_of_tiles_1557_5_895()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_585()) {
    jj_scanpos = xsp;
    if (jj_3_586()) return true;
    }
    return false;
  }

 inline bool jj_3_585()
 {
    if (jj_done) return true;
    if (jj_3R_value_expression_1855_5_178()) return true;
    return false;
  }

 inline bool jj_3R_statement_cursor_7696_5_874()
 {
    if (jj_done) return true;
    if (jj_3R_cursor_properties_6696_5_989()) return true;
    if (jj_scan_token(FOR)) return true;
    if (jj_3R_extended_identifier_1047_5_142()) return true;
    return false;
  }

 inline bool jj_3_573()
 {
    if (jj_done) return true;
    if (jj_scan_token(570)) return true;
    if (jj_3R_column_reference_1502_5_193()) return true;
    return false;
  }

 inline bool jj_3R_ntile_function_1550_5_214()
 {
    if (jj_done) return true;
    if (jj_scan_token(NTILE)) return true;
    if (jj_scan_token(lparen)) return true;
    if (jj_3R_number_of_tiles_1557_5_895()) return true;
    if (jj_scan_token(rparen)) return true;
    return false;
  }

 inline bool jj_3_2080()
 {
    if (jj_done) return true;
    if (jj_3R_result_set_cursor_7703_5_875()) return true;
    return false;
  }

 inline bool jj_3R_cursor_intent_7689_5_1005()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_2079()) {
    jj_scanpos = xsp;
    if (jj_3_2080()) return true;
    }
    return false;
  }

 inline bool jj_3_2079()
 {
    if (jj_done) return true;
    if (jj_3R_statement_cursor_7696_5_874()) return true;
    return false;
  }

 inline bool jj_3_584()
 {
    if (jj_done) return true;
    if (jj_scan_token(CUME_DIST)) return true;
    return false;
  }

 inline bool jj_3_583()
 {
    if (jj_done) return true;
    if (jj_scan_token(PERCENT_RANK)) return true;
    return false;
  }

 inline bool jj_3_582()
 {
    if (jj_done) return true;
    if (jj_scan_token(DENSE_RANK)) return true;
    return false;
  }

 inline bool jj_3_581()
 {
    if (jj_done) return true;
    if (jj_scan_token(RANK)) return true;
    return false;
  }

 inline bool jj_3R_rank_function_type_1541_5_213()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_581()) {
    jj_scanpos = xsp;
    if (jj_3_582()) {
    jj_scanpos = xsp;
    if (jj_3_583()) {
    jj_scanpos = xsp;
    if (jj_3_584()) return true;
    }
    }
    }
    return false;
  }

 inline bool jj_3R_allocate_cursor_statement_7683_5_801()
 {
    if (jj_done) return true;
    if (jj_scan_token(ALLOCATE)) return true;
    if (jj_3R_extended_cursor_name_1060_5_145()) return true;
    if (jj_3R_cursor_intent_7689_5_1005()) return true;
    return false;
  }

 inline bool jj_3_2077()
 {
    if (jj_done) return true;
    if (jj_3R_result_using_clause_7657_5_872()) return true;
    return false;
  }

 inline bool jj_3_580()
 {
    if (jj_done) return true;
    if (jj_3R_nth_value_function_1599_5_217()) return true;
    return false;
  }

 inline bool jj_3_579()
 {
    if (jj_done) return true;
    if (jj_3R_first_or_last_value_function_1587_5_216()) return true;
    return false;
  }

 inline bool jj_3_578()
 {
    if (jj_done) return true;
    if (jj_3R_lead_or_lag_function_1564_5_215()) return true;
    return false;
  }

 inline bool jj_3R_dynamic_declare_cursor_7675_5_723()
 {
    if (jj_done) return true;
    if (jj_scan_token(DECLARE)) return true;
    if (jj_3R_cursor_name_995_5_144()) return true;
    if (jj_3R_cursor_properties_6696_5_989()) return true;
    return false;
  }

 inline bool jj_3_577()
 {
    if (jj_done) return true;
    if (jj_3R_ntile_function_1550_5_214()) return true;
    return false;
  }

 inline bool jj_3_576()
 {
    if (jj_done) return true;
    if (jj_3R_aggregate_function_4109_3_211()) return true;
    return false;
  }

 inline bool jj_3_575()
 {
    if (jj_done) return true;
    if (jj_scan_token(ROW_NUMBER)) return true;
    if (jj_scan_token(lparen)) return true;
    if (jj_scan_token(rparen)) return true;
    return false;
  }

 inline bool jj_3_574()
 {
    if (jj_done) return true;
    if (jj_3R_rank_function_type_1541_5_213()) return true;
    if (jj_scan_token(lparen)) return true;
    if (jj_scan_token(rparen)) return true;
    return false;
  }

 inline bool jj_3R_window_function_type_1529_5_190()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_574()) {
    jj_scanpos = xsp;
    if (jj_3_575()) {
    jj_scanpos = xsp;
    if (jj_3_576()) {
    jj_scanpos = xsp;
    if (jj_3_577()) {
    jj_scanpos = xsp;
    if (jj_3_578()) {
    jj_scanpos = xsp;
    if (jj_3_579()) {
    jj_scanpos = xsp;
    if (jj_3_580()) return true;
    }
    }
    }
    }
    }
    }
    return false;
  }

 inline bool jj_3R_execute_immediate_statement_7669_5_799()
 {
    if (jj_done) return true;
    if (jj_scan_token(EXECUTE)) return true;
    if (jj_scan_token(IMMEDIATE)) return true;
    if (jj_3R_simple_value_specification_1423_5_220()) return true;
    return false;
  }

 inline bool jj_3R_window_function_1522_5_198()
 {
    if (jj_done) return true;
    if (jj_scan_token(OVER)) return true;
    if (jj_3R_window_name_or_specification_1619_5_894()) return true;
    return false;
  }

 inline bool jj_3R_parameter_using_clause_7663_5_873()
 {
    if (jj_done) return true;
    if (jj_3R_input_using_clause_7601_5_876()) return true;
    return false;
  }

 inline bool jj_3R_grouping_operation_1516_5_212()
 {
    if (jj_done) return true;
    if (jj_scan_token(GROUPING)) return true;
    if (jj_scan_token(lparen)) return true;
    if (jj_3R_column_reference_1502_5_193()) return true;
    Token * xsp;
    while (true) {
      xsp = jj_scanpos;
      if (jj_3_573()) { jj_scanpos = xsp; break; }
    }
    if (jj_scan_token(rparen)) return true;
    return false;
  }

 inline bool jj_3_2075()
 {
    if (jj_done) return true;
    if (jj_scan_token(570)) return true;
    if (jj_3R_into_argument_7639_5_871()) return true;
    return false;
  }

 inline bool jj_3R_result_using_clause_7657_5_872()
 {
    if (jj_done) return true;
    if (jj_3R_output_using_clause_7626_5_1007()) return true;
    return false;
  }

 inline bool jj_3_568()
 {
    if (jj_done) return true;
    if (jj_scan_token(569)) return true;
    if (jj_3R_identifier_928_3_141()) return true;
    return false;
  }

 inline bool jj_3_2076()
 {
    if (jj_done) return true;
    if (jj_scan_token(SQL)) return true;
    return false;
  }

 inline bool jj_3_572()
 {
    if (jj_done) return true;
    if (jj_3R_grouping_operation_1516_5_212()) return true;
    return false;
  }

 inline bool jj_3_571()
 {
    if (jj_done) return true;
    if (jj_3R_aggregate_function_4109_3_211()) return true;
    return false;
  }

 inline bool jj_3R_set_function_specification_1509_5_180()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_571()) {
    jj_scanpos = xsp;
    if (jj_3_572()) return true;
    }
    return false;
  }

 inline bool jj_3R_execute_statement_7651_5_798()
 {
    if (jj_done) return true;
    if (jj_scan_token(EXECUTE)) return true;
    if (jj_3R_SQL_identifier_1040_5_865()) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_2077()) jj_scanpos = xsp;
    xsp = jj_scanpos;
    if (jj_3_2078()) jj_scanpos = xsp;
    return false;
  }

 inline bool jj_3_570()
 {
    if (jj_done) return true;
    if (jj_scan_token(MODULE)) return true;
    if (jj_scan_token(569)) return true;
    if (jj_3R_identifier_928_3_141()) return true;
    if (jj_scan_token(569)) return true;
    if (jj_3R_identifier_928_3_141()) return true;
    return false;
  }

 inline bool jj_3R_into_descriptor_7645_5_870()
 {
    if (jj_done) return true;
    if (jj_scan_token(INTO)) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_2076()) jj_scanpos = xsp;
    if (jj_scan_token(DESCRIPTOR)) return true;
    if (jj_3R_descriptor_name_1066_5_1008()) return true;
    return false;
  }

 inline bool jj_3_569()
 {
    if (jj_done) return true;
    if (jj_3R_identifier_chain_1496_5_207()) return true;
    return false;
  }

 inline bool jj_3R_column_reference_1502_5_193()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_569()) {
    jj_scanpos = xsp;
    if (jj_3_570()) return true;
    }
    return false;
  }

 inline bool jj_3R_into_argument_7639_5_871()
 {
    if (jj_done) return true;
    if (jj_3R_target_specification_1434_3_475()) return true;
    return false;
  }

 inline bool jj_3R_identifier_chain_1496_5_207()
 {
    if (jj_done) return true;
    if (jj_3R_identifier_928_3_141()) return true;
    Token * xsp;
    while (true) {
      xsp = jj_scanpos;
      if (jj_3_568()) { jj_scanpos = xsp; break; }
    }
    return false;
  }

 inline bool jj_3_2064()
 {
    if (jj_done) return true;
    if (jj_3R_nesting_option_7581_5_864()) return true;
    return false;
  }

 inline bool jj_3_2072()
 {
    if (jj_done) return true;
    if (jj_scan_token(570)) return true;
    if (jj_3R_using_argument_7614_5_868()) return true;
    return false;
  }

 inline bool jj_3_567()
 {
    if (jj_done) return true;
    if (jj_scan_token(MULTISET)) return true;
    if (jj_3R_left_bracket_or_trigraph_804_5_174()) return true;
    if (jj_3R_right_bracket_or_trigraph_811_5_175()) return true;
    return false;
  }

 inline bool jj_3R_into_arguments_7633_5_869()
 {
    if (jj_done) return true;
    if (jj_scan_token(INTO)) return true;
    if (jj_3R_into_argument_7639_5_871()) return true;
    Token * xsp;
    while (true) {
      xsp = jj_scanpos;
      if (jj_3_2075()) { jj_scanpos = xsp; break; }
    }
    return false;
  }

 inline bool jj_3_566()
 {
    if (jj_done) return true;
    if (jj_scan_token(ARRAY)) return true;
    if (jj_3R_left_bracket_or_trigraph_804_5_174()) return true;
    if (jj_3R_right_bracket_or_trigraph_811_5_175()) return true;
    return false;
  }

 inline bool jj_3R_empty_specification_1489_5_210()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_566()) {
    jj_scanpos = xsp;
    if (jj_3_567()) return true;
    }
    return false;
  }

 inline bool jj_3_2074()
 {
    if (jj_done) return true;
    if (jj_3R_into_descriptor_7645_5_870()) return true;
    return false;
  }

 inline bool jj_3R_output_using_clause_7626_5_1007()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_2073()) {
    jj_scanpos = xsp;
    if (jj_3_2074()) return true;
    }
    return false;
  }

 inline bool jj_3_565()
 {
    if (jj_done) return true;
    if (jj_3R_empty_specification_1489_5_210()) return true;
    return false;
  }

 inline bool jj_3_2062()
 {
    if (jj_done) return true;
    if (jj_3R_nesting_option_7581_5_864()) return true;
    return false;
  }

 inline bool jj_3_2073()
 {
    if (jj_done) return true;
    if (jj_3R_into_arguments_7633_5_869()) return true;
    return false;
  }

 inline bool jj_3_564()
 {
    if (jj_done) return true;
    if (jj_scan_token(NULL_)) return true;
    return false;
  }

 inline bool jj_3R_implicitly_typed_value_specification_1482_5_209()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_564()) {
    jj_scanpos = xsp;
    if (jj_3_565()) return true;
    }
    return false;
  }

 inline bool jj_3R_using_input_descriptor_7620_5_867()
 {
    if (jj_done) return true;
    if (jj_3R_using_descriptor_7588_5_1020()) return true;
    return false;
  }

 inline bool jj_3_563()
 {
    if (jj_done) return true;
    if (jj_scan_token(DEFAULT_)) return true;
    return false;
  }

 inline bool jj_3_562()
 {
    if (jj_done) return true;
    if (jj_3R_implicitly_typed_value_specification_1482_5_209()) return true;
    return false;
  }

 inline bool jj_3R_contextually_typed_value_specification_1475_5_194()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_562()) {
    jj_scanpos = xsp;
    if (jj_3_563()) return true;
    }
    return false;
  }

 inline bool jj_3R_using_argument_7614_5_868()
 {
    if (jj_done) return true;
    if (jj_3R_general_value_specification_1393_5_204()) return true;
    return false;
  }

 inline bool jj_3R_current_collation_specification_1469_5_206()
 {
    if (jj_done) return true;
    if (jj_scan_token(COLLATION)) return true;
    if (jj_scan_token(FOR)) return true;
    if (jj_scan_token(lparen)) return true;
    if (jj_3R_string_value_expression_2121_5_262()) return true;
    if (jj_scan_token(rparen)) return true;
    return false;
  }

 inline bool jj_3R_using_arguments_7608_5_866()
 {
    if (jj_done) return true;
    if (jj_scan_token(USING)) return true;
    if (jj_3R_using_argument_7614_5_868()) return true;
    Token * xsp;
    while (true) {
      xsp = jj_scanpos;
      if (jj_3_2072()) { jj_scanpos = xsp; break; }
    }
    return false;
  }

 inline bool jj_3R_target_array_element_specification_1463_5_208()
 {
    if (jj_done) return true;
    if (jj_3R_left_bracket_or_trigraph_804_5_174()) return true;
    if (jj_3R_simple_value_specification_1423_5_220()) return true;
    if (jj_3R_right_bracket_or_trigraph_811_5_175()) return true;
    return false;
  }

 inline bool jj_3_2071()
 {
    if (jj_done) return true;
    if (jj_3R_using_input_descriptor_7620_5_867()) return true;
    return false;
  }

 inline bool jj_3R_input_using_clause_7601_5_876()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_2070()) {
    jj_scanpos = xsp;
    if (jj_3_2071()) return true;
    }
    return false;
  }

 inline bool jj_3_2070()
 {
    if (jj_done) return true;
    if (jj_3R_using_arguments_7608_5_866()) return true;
    return false;
  }

 inline bool jj_3_2067()
 {
    if (jj_done) return true;
    if (jj_scan_token(SQL)) return true;
    return false;
  }

 inline bool jj_3_561()
 {
    if (jj_done) return true;
    if (jj_3R_column_reference_1502_5_193()) return true;
    return false;
  }

 inline bool jj_3R_simple_target_specification_1452_5_1022()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_560()) {
    jj_scanpos = xsp;
    if (jj_3_561()) return true;
    }
    return false;
  }

 inline bool jj_3_560()
 {
    if (jj_done) return true;
    if (jj_3R_identifier_chain_1496_5_207()) return true;
    return false;
  }

 inline bool jj_3_2069()
 {
    if (jj_done) return true;
    if (jj_scan_token(CURSOR)) return true;
    if (jj_3R_extended_cursor_name_1060_5_145()) return true;
    if (jj_scan_token(STRUCTURE)) return true;
    return false;
  }

 inline bool jj_3R_described_object_7594_5_1019()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_2068()) {
    jj_scanpos = xsp;
    if (jj_3_2069()) return true;
    }
    return false;
  }

 inline bool jj_3_2068()
 {
    if (jj_done) return true;
    if (jj_3R_SQL_identifier_1040_5_865()) return true;
    return false;
  }

 inline bool jj_3R_using_descriptor_7588_5_1020()
 {
    if (jj_done) return true;
    if (jj_scan_token(USING)) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_2067()) jj_scanpos = xsp;
    if (jj_scan_token(DESCRIPTOR)) return true;
    if (jj_3R_descriptor_name_1066_5_1008()) return true;
    return false;
  }

 inline bool jj_3_2063()
 {
    if (jj_done) return true;
    if (jj_scan_token(OUTPUT)) return true;
    return false;
  }

 inline bool jj_3_558()
 {
    if (jj_done) return true;
    if (jj_scan_token(571)) return true;
    return false;
  }

 inline bool jj_3_557()
 {
    if (jj_done) return true;
    if (jj_3R_target_array_element_specification_1463_5_208()) return true;
    return false;
  }

 inline bool jj_3_559()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_557()) {
    jj_scanpos = xsp;
    if (jj_3_558()) return true;
    }
    return false;
  }

 inline bool jj_3_2066()
 {
    if (jj_done) return true;
    if (jj_scan_token(WITHOUT)) return true;
    if (jj_scan_token(NESTING)) return true;
    return false;
  }

 inline bool jj_3R_nesting_option_7581_5_864()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_2065()) {
    jj_scanpos = xsp;
    if (jj_3_2066()) return true;
    }
    return false;
  }

 inline bool jj_3_2065()
 {
    if (jj_done) return true;
    if (jj_scan_token(WITH)) return true;
    if (jj_scan_token(NESTING)) return true;
    return false;
  }

 inline bool jj_3_556()
 {
    if (jj_done) return true;
    if (jj_3R_column_reference_1502_5_193()) return true;
    return false;
  }

 inline bool jj_3_555()
 {
    if (jj_done) return true;
    if (jj_3R_identifier_chain_1496_5_207()) return true;
    return false;
  }

 inline bool jj_3R_describe_output_statement_7575_5_863()
 {
    if (jj_done) return true;
    if (jj_scan_token(DESCRIBE)) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_2063()) jj_scanpos = xsp;
    if (jj_3R_described_object_7594_5_1019()) return true;
    if (jj_3R_using_descriptor_7588_5_1020()) return true;
    return false;
  }

 inline bool jj_3R_target_specification_1434_3_475()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_555()) {
    jj_scanpos = xsp;
    if (jj_3_556()) return true;
    }
    xsp = jj_scanpos;
    if (jj_3_559()) jj_scanpos = xsp;
    return false;
  }

 inline bool jj_3R_describe_input_statement_7569_5_862()
 {
    if (jj_done) return true;
    if (jj_scan_token(DESCRIBE)) return true;
    if (jj_scan_token(INPUT)) return true;
    if (jj_3R_SQL_identifier_1040_5_865()) return true;
    return false;
  }

 inline bool jj_3_554()
 {
    if (jj_done) return true;
    if (jj_3R_identifier_chain_1496_5_207()) return true;
    return false;
  }

 inline bool jj_3_553()
 {
    if (jj_done) return true;
    if (jj_3R_literal_818_5_203()) return true;
    return false;
  }

 inline bool jj_3R_simple_value_specification_1423_5_220()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_553()) {
    jj_scanpos = xsp;
    if (jj_3_554()) return true;
    }
    return false;
  }

 inline bool jj_3_2061()
 {
    if (jj_done) return true;
    if (jj_3R_describe_output_statement_7575_5_863()) return true;
    return false;
  }

 inline bool jj_3R_describe_statement_7562_5_797()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_2060()) {
    jj_scanpos = xsp;
    if (jj_3_2061()) return true;
    }
    return false;
  }

 inline bool jj_3_2060()
 {
    if (jj_done) return true;
    if (jj_3R_describe_input_statement_7569_5_862()) return true;
    return false;
  }

 inline bool jj_3_549()
 {
    if (jj_done) return true;
    if (jj_scan_token(CURRENT_TRANSFORM_GROUP_FOR_TYPE)) return true;
    if (jj_3R_path_resolved_user_defined_type_name_1281_5_151()) return true;
    return false;
  }

 inline bool jj_3R_deallocate_prepared_statement_7556_5_796()
 {
    if (jj_done) return true;
    if (jj_scan_token(DEALLOCATE)) return true;
    if (jj_scan_token(PREPARE)) return true;
    if (jj_3R_SQL_identifier_1040_5_865()) return true;
    return false;
  }

 inline bool jj_3_548()
 {
    if (jj_done) return true;
    if (jj_scan_token(CURRENT_DEFAULT_TRANSFORM_GROUP)) return true;
    return false;
  }

 inline bool jj_3_547()
 {
    if (jj_done) return true;
    if (jj_scan_token(VALUE)) return true;
    return false;
  }

 inline bool jj_3_546()
 {
    if (jj_done) return true;
    if (jj_scan_token(CURRENT_SCHEMA)) return true;
    return false;
  }

 inline bool jj_3_545()
 {
    if (jj_done) return true;
    if (jj_scan_token(CURRENT_ROLE)) return true;
    return false;
  }

 inline bool jj_3_544()
 {
    if (jj_done) return true;
    if (jj_scan_token(CURRENT_PATH)) return true;
    return false;
  }

 inline bool jj_3_543()
 {
    if (jj_done) return true;
    if (jj_scan_token(CURRENT_CATALOG)) return true;
    return false;
  }

 inline bool jj_3_542()
 {
    if (jj_done) return true;
    if (jj_scan_token(SYSTEM_USER)) return true;
    return false;
  }

 inline bool jj_3_2059()
 {
    if (jj_done) return true;
    if (jj_3R_cursor_returnability_6726_5_814()) return true;
    return false;
  }

 inline bool jj_3_541()
 {
    if (jj_done) return true;
    if (jj_scan_token(SESSION_USER)) return true;
    return false;
  }

 inline bool jj_3_2058()
 {
    if (jj_done) return true;
    if (jj_3R_cursor_holdability_6719_5_813()) return true;
    return false;
  }

 inline bool jj_3_540()
 {
    if (jj_done) return true;
    if (jj_3R_current_collation_specification_1469_5_206()) return true;
    return false;
  }

 inline bool jj_3_2057()
 {
    if (jj_done) return true;
    if (jj_3R_cursor_scrollability_6712_5_812()) return true;
    return false;
  }

 inline bool jj_3R_cursor_attribute_7547_5_861()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_2056()) {
    jj_scanpos = xsp;
    if (jj_3_2057()) {
    jj_scanpos = xsp;
    if (jj_3_2058()) {
    jj_scanpos = xsp;
    if (jj_3_2059()) return true;
    }
    }
    }
    return false;
  }

 inline bool jj_3_539()
 {
    if (jj_done) return true;
    if (jj_scan_token(571)) return true;
    return false;
  }

 inline bool jj_3_2056()
 {
    if (jj_done) return true;
    if (jj_3R_cursor_sensitivity_6704_5_811()) return true;
    return false;
  }

 inline bool jj_3_2055()
 {
    if (jj_done) return true;
    if (jj_3R_cursor_attribute_7547_5_861()) return true;
    return false;
  }

 inline bool jj_3_538()
 {
    if (jj_done) return true;
    if (jj_scan_token(USER)) return true;
    return false;
  }

 inline bool jj_3_552()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_539()) {
    jj_scanpos = xsp;
    if (jj_3_540()) {
    jj_scanpos = xsp;
    if (jj_3_541()) {
    jj_scanpos = xsp;
    if (jj_3_542()) {
    jj_scanpos = xsp;
    if (jj_3_543()) {
    jj_scanpos = xsp;
    if (jj_3_544()) {
    jj_scanpos = xsp;
    if (jj_3_545()) {
    jj_scanpos = xsp;
    if (jj_3_546()) {
    jj_scanpos = xsp;
    if (jj_3_547()) {
    jj_scanpos = xsp;
    if (jj_3_548()) {
    jj_scanpos = xsp;
    if (jj_3_549()) return true;
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    return false;
  }

 inline bool jj_3_537()
 {
    if (jj_done) return true;
    if (jj_scan_token(CURRENT_USER)) return true;
    return false;
  }

 inline bool jj_3_550()
 {
    if (jj_done) return true;
    if (jj_3R_identifier_chain_1496_5_207()) return true;
    return false;
  }

 inline bool jj_3_551()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_537()) {
    jj_scanpos = xsp;
    if (jj_3_538()) return true;
    }
    return false;
  }

 inline bool jj_3R_general_value_specification_1393_5_204()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_550()) {
    jj_scanpos = xsp;
    if (jj_3_551()) {
    jj_scanpos = xsp;
    if (jj_3_552()) return true;
    }
    }
    return false;
  }

 inline bool jj_3R_preparable_implementation_defined_statement_7535_5_856()
 {
    if (jj_done) return true;
    if (jj_3R_character_string_literal_866_3_128()) return true;
    return false;
  }

 inline bool jj_3_536()
 {
    if (jj_done) return true;
    if (jj_3R_general_value_specification_1393_5_204()) return true;
    return false;
  }

 inline bool jj_3_535()
 {
    if (jj_done) return true;
    if (jj_3R_unsigned_literal_832_5_205()) return true;
    return false;
  }

 inline bool jj_3R_unsigned_value_specification_1386_5_192()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_535()) {
    jj_scanpos = xsp;
    if (jj_3_536()) return true;
    }
    return false;
  }

 inline bool jj_3R_dynamic_select_statement_7528_5_858()
 {
    if (jj_done) return true;
    if (jj_3R_cursor_specification_6733_5_1018()) return true;
    return false;
  }

 inline bool jj_3_534()
 {
    if (jj_done) return true;
    if (jj_3R_general_value_specification_1393_5_204()) return true;
    return false;
  }

 inline bool jj_3R_preparable_SQL_session_statement_7522_5_855()
 {
    if (jj_done) return true;
    if (jj_3R_SQL_session_statement_6636_5_734()) return true;
    return false;
  }

 inline bool jj_3R_value_specification_1379_5_844()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_533()) {
    jj_scanpos = xsp;
    if (jj_3_534()) return true;
    }
    return false;
  }

 inline bool jj_3_533()
 {
    if (jj_done) return true;
    if (jj_3R_literal_818_5_203()) return true;
    return false;
  }

 inline bool jj_3R_preparable_SQL_control_statement_7516_5_854()
 {
    if (jj_done) return true;
    if (jj_3R_SQL_control_statement_6609_5_731()) return true;
    return false;
  }

 inline bool jj_3_532()
 {
    if (jj_done) return true;
    if (jj_3R_multiset_value_constructor_2637_5_202()) return true;
    return false;
  }

 inline bool jj_3_531()
 {
    if (jj_done) return true;
    if (jj_3R_array_value_constructor_2579_5_201()) return true;
    return false;
  }

 inline bool jj_3R_collection_value_constructor_1372_5_187()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_531()) {
    jj_scanpos = xsp;
    if (jj_3_532()) return true;
    }
    return false;
  }

 inline bool jj_3R_preparable_SQL_transaction_statement_7510_5_853()
 {
    if (jj_done) return true;
    if (jj_3R_SQL_transaction_statement_6616_5_732()) return true;
    return false;
  }

 inline bool jj_3_530()
 {
    if (jj_done) return true;
    if (jj_3R_static_method_invocation_1800_5_200()) return true;
    return false;
  }

 inline bool jj_3_529()
 {
    if (jj_done) return true;
    if (jj_3R_array_element_reference_1842_5_199()) return true;
    return false;
  }

 inline bool jj_3_528()
 {
    if (jj_done) return true;
    if (jj_3R_window_function_1522_5_198()) return true;
    return false;
  }

 inline bool jj_3_527()
 {
    if (jj_done) return true;
    if (jj_3R_method_invocation_1777_4_197()) return true;
    return false;
  }

 inline bool jj_3R_preparable_SQL_schema_statement_7504_5_852()
 {
    if (jj_done) return true;
    if (jj_3R_SQL_schema_statement_6526_5_729()) return true;
    return false;
  }

 inline bool jj_3_526()
 {
    if (jj_done) return true;
    if (jj_3R_attribute_or_method_reference_1821_5_196()) return true;
    return false;
  }

 inline bool jj_3_525()
 {
    if (jj_done) return true;
    if (jj_3R_field_reference_1757_5_195()) return true;
    return false;
  }

 inline bool jj_3R_primary_suffix_1358_3_179()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_525()) {
    jj_scanpos = xsp;
    if (jj_3_526()) {
    jj_scanpos = xsp;
    if (jj_3_527()) {
    jj_scanpos = xsp;
    if (jj_3_528()) {
    jj_scanpos = xsp;
    if (jj_3_529()) {
    jj_scanpos = xsp;
    if (jj_3_530()) return true;
    }
    }
    }
    }
    }
    return false;
  }

 inline bool jj_3_522()
 {
    if (jj_done) return true;
    if (jj_3R_primary_suffix_1358_3_179()) return true;
    return false;
  }

 inline bool jj_3_2054()
 {
    if (jj_done) return true;
    if (jj_3R_preparable_dynamic_update_statement_positioned_7759_5_860()) return true;
    return false;
  }

 inline bool jj_3_521()
 {
    if (jj_done) return true;
    if (jj_3R_column_reference_1502_5_193()) return true;
    return false;
  }

 inline bool jj_3_2053()
 {
    if (jj_done) return true;
    if (jj_3R_preparable_dynamic_delete_statement_positioned_7746_5_859()) return true;
    return false;
  }

 inline bool jj_3_520()
 {
    if (jj_done) return true;
    if (jj_3R_unsigned_value_specification_1386_5_192()) return true;
    return false;
  }

 inline bool jj_3_2052()
 {
    if (jj_done) return true;
    if (jj_3R_merge_statement_6878_5_385()) return true;
    return false;
  }

 inline bool jj_3_519()
 {
    if (jj_done) return true;
    if (jj_scan_token(lparen)) return true;
    if (jj_3R_column_name_list_2942_5_191()) return true;
    if (jj_scan_token(rparen)) return true;
    return false;
  }

 inline bool jj_3_2039()
 {
    if (jj_done) return true;
    if (jj_3R_attributes_specification_7470_5_850()) return true;
    return false;
  }

 inline bool jj_3_2051()
 {
    if (jj_done) return true;
    if (jj_3R_truncate_table_statement_6810_5_771()) return true;
    return false;
  }

 inline bool jj_3_518()
 {
    if (jj_done) return true;
    if (jj_3R_window_function_type_1529_5_190()) return true;
    return false;
  }

 inline bool jj_3_2050()
 {
    if (jj_done) return true;
    if (jj_3R_update_statement_searched_6963_5_386()) return true;
    return false;
  }

 inline bool jj_3_2049()
 {
    if (jj_done) return true;
    if (jj_3R_dynamic_select_statement_7528_5_858()) return true;
    return false;
  }

 inline bool jj_3_517()
 {
    if (jj_done) return true;
    if (jj_3R_next_value_expression_1751_5_189()) return true;
    return false;
  }

 inline bool jj_3_2048()
 {
    if (jj_done) return true;
    if (jj_3R_insert_statement_6823_5_384()) return true;
    return false;
  }

 inline bool jj_3_516()
 {
    if (jj_done) return true;
    if (jj_3R_multiset_element_reference_1849_5_188()) return true;
    return false;
  }

 inline bool jj_3_2047()
 {
    if (jj_done) return true;
    if (jj_3R_dynamic_single_row_select_statement_7721_5_857()) return true;
    return false;
  }

 inline bool jj_3R_preparable_SQL_data_statement_7487_5_851()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_2046()) {
    jj_scanpos = xsp;
    if (jj_3_2047()) {
    jj_scanpos = xsp;
    if (jj_3_2048()) {
    jj_scanpos = xsp;
    if (jj_3_2049()) {
    jj_scanpos = xsp;
    if (jj_3_2050()) {
    jj_scanpos = xsp;
    if (jj_3_2051()) {
    jj_scanpos = xsp;
    if (jj_3_2052()) {
    jj_scanpos = xsp;
    if (jj_3_2053()) {
    jj_scanpos = xsp;
    if (jj_3_2054()) return true;
    }
    }
    }
    }
    }
    }
    }
    }
    return false;
  }

 inline bool jj_3_515()
 {
    if (jj_done) return true;
    if (jj_3R_collection_value_constructor_1372_5_187()) return true;
    return false;
  }

 inline bool jj_3_2046()
 {
    if (jj_done) return true;
    if (jj_3R_delete_statement_searched_6803_5_383()) return true;
    return false;
  }

 inline bool jj_3_514()
 {
    if (jj_done) return true;
    if (jj_3R_reference_resolution_1836_5_186()) return true;
    return false;
  }

 inline bool jj_3_513()
 {
    if (jj_done) return true;
    if (jj_3R_new_specification_1808_5_185()) return true;
    return false;
  }

 inline bool jj_3_512()
 {
    if (jj_done) return true;
    if (jj_3R_subtype_treatment_1764_5_184()) return true;
    return false;
  }

 inline bool jj_3_511()
 {
    if (jj_done) return true;
    if (jj_3R_cast_specification_1730_5_183()) return true;
    return false;
  }

 inline bool jj_3_510()
 {
    if (jj_done) return true;
    if (jj_3R_case_expression_1632_5_182()) return true;
    return false;
  }

 inline bool jj_3_509()
 {
    if (jj_done) return true;
    if (jj_3R_subquery_3527_5_181()) return true;
    return false;
  }

 inline bool jj_3_2045()
 {
    if (jj_done) return true;
    if (jj_3R_preparable_implementation_defined_statement_7535_5_856()) return true;
    return false;
  }

 inline bool jj_3_508()
 {
    if (jj_done) return true;
    if (jj_3R_set_function_specification_1509_5_180()) return true;
    return false;
  }

 inline bool jj_3_2044()
 {
    if (jj_done) return true;
    if (jj_3R_preparable_SQL_session_statement_7522_5_855()) return true;
    return false;
  }

 inline bool jj_3_2043()
 {
    if (jj_done) return true;
    if (jj_3R_preparable_SQL_control_statement_7516_5_854()) return true;
    return false;
  }

 inline bool jj_3_2042()
 {
    if (jj_done) return true;
    if (jj_3R_preparable_SQL_transaction_statement_7510_5_853()) return true;
    return false;
  }

 inline bool jj_3_524()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_508()) {
    jj_scanpos = xsp;
    if (jj_3_509()) {
    jj_scanpos = xsp;
    if (jj_3_510()) {
    jj_scanpos = xsp;
    if (jj_3_511()) {
    jj_scanpos = xsp;
    if (jj_3_512()) {
    jj_scanpos = xsp;
    if (jj_3_513()) {
    jj_scanpos = xsp;
    if (jj_3_514()) {
    jj_scanpos = xsp;
    if (jj_3_515()) {
    jj_scanpos = xsp;
    if (jj_3_516()) {
    jj_scanpos = xsp;
    if (jj_3_517()) {
    jj_scanpos = xsp;
    if (jj_3_518()) {
    jj_scanpos = xsp;
    if (jj_3_519()) {
    jj_scanpos = xsp;
    if (jj_3_520()) {
    jj_scanpos = xsp;
    if (jj_3_521()) return true;
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    while (true) {
      xsp = jj_scanpos;
      if (jj_3_522()) { jj_scanpos = xsp; break; }
    }
    return false;
  }

 inline bool jj_3_2041()
 {
    if (jj_done) return true;
    if (jj_3R_preparable_SQL_schema_statement_7504_5_852()) return true;
    return false;
  }

 inline bool jj_3_523()
 {
    if (jj_done) return true;
    if (jj_3R_contextually_typed_value_specification_1475_5_194()) return true;
    return false;
  }

 inline bool jj_3R_nonparenthesized_value_expression_primary_1333_5_177()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_523()) {
    jj_scanpos = xsp;
    if (jj_3_524()) return true;
    }
    return false;
  }

 inline bool jj_3_2040()
 {
    if (jj_done) return true;
    if (jj_3R_preparable_SQL_data_statement_7487_5_851()) return true;
    return false;
  }

 inline bool jj_3R_attributes_specification_7470_5_850()
 {
    if (jj_done) return true;
    if (jj_scan_token(ATTRIBUTES)) return true;
    if (jj_3R_simple_value_specification_1423_5_220()) return true;
    return false;
  }

 inline bool jj_3_506()
 {
    if (jj_done) return true;
    if (jj_scan_token(570)) return true;
    if (jj_3R_value_expression_1855_5_178()) return true;
    return false;
  }

 inline bool jj_3_507()
 {
    if (jj_done) return true;
    if (jj_3R_primary_suffix_1358_3_179()) return true;
    return false;
  }

 inline bool jj_3_2035()
 {
    if (jj_done) return true;
    if (jj_scan_token(570)) return true;
    if (jj_3R_set_header_information_7451_5_848()) return true;
    return false;
  }

 inline bool jj_3R_prepare_statement_7463_5_795()
 {
    if (jj_done) return true;
    if (jj_scan_token(PREPARE)) return true;
    if (jj_3R_SQL_identifier_1040_5_865()) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_2039()) jj_scanpos = xsp;
    if (jj_scan_token(FROM)) return true;
    return false;
  }

 inline bool jj_3R_parenthesized_value_expression_1322_3_176()
 {
    if (jj_done) return true;
    if (jj_scan_token(lparen)) return true;
    if (jj_3R_value_expression_1855_5_178()) return true;
    Token * xsp;
    while (true) {
      xsp = jj_scanpos;
      if (jj_3_506()) { jj_scanpos = xsp; break; }
    }
    if (jj_scan_token(rparen)) return true;
    while (true) {
      xsp = jj_scanpos;
      if (jj_3_507()) { jj_scanpos = xsp; break; }
    }
    return false;
  }

 inline bool jj_3_505()
 {
    if (jj_done) return true;
    if (jj_3R_nonparenthesized_value_expression_primary_1333_5_177()) return true;
    return false;
  }

 inline bool jj_3R_value_expression_primary_1315_5_349()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_504()) {
    jj_scanpos = xsp;
    if (jj_3_505()) return true;
    }
    return false;
  }

 inline bool jj_3_504()
 {
    if (jj_done) return true;
    if (jj_3R_parenthesized_value_expression_1322_3_176()) return true;
    return false;
  }

 inline bool jj_3R_set_item_information_7457_5_849()
 {
    if (jj_done) return true;
    if (jj_3R_descriptor_item_name_7395_5_1017()) return true;
    if (jj_scan_token(EQUAL)) return true;
    return false;
  }

 inline bool jj_3R_field_definition_1309_5_170()
 {
    if (jj_done) return true;
    if (jj_3R_identifier_928_3_141()) return true;
    if (jj_3R_data_type_1086_3_251()) return true;
    return false;
  }

 inline bool jj_3R_set_header_information_7451_5_848()
 {
    if (jj_done) return true;
    if (jj_3R_header_item_name_7367_5_1016()) return true;
    if (jj_scan_token(EQUAL)) return true;
    if (jj_3R_simple_value_specification_1423_5_220()) return true;
    return false;
  }

 inline bool jj_3_2036()
 {
    if (jj_done) return true;
    if (jj_scan_token(570)) return true;
    if (jj_3R_set_item_information_7457_5_849()) return true;
    return false;
  }

 inline bool jj_3R_multiset_type_1302_5_173()
 {
    if (jj_done) return true;
    if (jj_scan_token(MULTISET)) return true;
    return false;
  }

 inline bool jj_3_2034()
 {
    if (jj_done) return true;
    if (jj_scan_token(SQL)) return true;
    return false;
  }

 inline bool jj_3_2038()
 {
    if (jj_done) return true;
    if (jj_scan_token(VALUE)) return true;
    if (jj_3R_simple_value_specification_1423_5_220()) return true;
    if (jj_3R_set_item_information_7457_5_849()) return true;
    return false;
  }

 inline bool jj_3_2037()
 {
    if (jj_done) return true;
    if (jj_3R_set_header_information_7451_5_848()) return true;
    return false;
  }

 inline bool jj_3_503()
 {
    if (jj_done) return true;
    if (jj_3R_left_bracket_or_trigraph_804_5_174()) return true;
    if (jj_scan_token(unsigned_integer)) return true;
    if (jj_3R_right_bracket_or_trigraph_811_5_175()) return true;
    return false;
  }

 inline bool jj_3_500()
 {
    if (jj_done) return true;
    if (jj_3R_scope_clause_1269_5_171()) return true;
    return false;
  }

 inline bool jj_3R_set_descriptor_statement_7437_5_809()
 {
    if (jj_done) return true;
    if (jj_scan_token(SET)) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_2034()) jj_scanpos = xsp;
    if (jj_scan_token(DESCRIPTOR)) return true;
    if (jj_3R_descriptor_name_1066_5_1008()) return true;
    return false;
  }

 inline bool jj_3R_array_type_1294_5_172()
 {
    if (jj_done) return true;
    if (jj_scan_token(ARRAY)) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_503()) jj_scanpos = xsp;
    return false;
  }

 inline bool jj_3_502()
 {
    if (jj_done) return true;
    if (jj_3R_multiset_type_1302_5_173()) return true;
    return false;
  }

 inline bool jj_3_2033()
 {
    if (jj_done) return true;
    if (jj_scan_token(USER_DEFINED_TYPE_CODE)) return true;
    return false;
  }

 inline bool jj_3_501()
 {
    if (jj_done) return true;
    if (jj_3R_array_type_1294_5_172()) return true;
    return false;
  }

 inline bool jj_3R_collection_type_1287_5_152()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_501()) {
    jj_scanpos = xsp;
    if (jj_3_502()) return true;
    }
    return false;
  }

 inline bool jj_3_2032()
 {
    if (jj_done) return true;
    if (jj_scan_token(USER_DEFINED_TYPE_SCHEMA)) return true;
    return false;
  }

 inline bool jj_3_2031()
 {
    if (jj_done) return true;
    if (jj_scan_token(USER_DEFINED_TYPE_NAME)) return true;
    return false;
  }

 inline bool jj_3_2030()
 {
    if (jj_done) return true;
    if (jj_scan_token(USER_DEFINED_TYPE_CATALOG)) return true;
    return false;
  }

 inline bool jj_3_2029()
 {
    if (jj_done) return true;
    if (jj_scan_token(UNNAMED)) return true;
    return false;
  }

 inline bool jj_3_499()
 {
    if (jj_done) return true;
    if (jj_scan_token(570)) return true;
    if (jj_3R_field_definition_1309_5_170()) return true;
    return false;
  }

 inline bool jj_3_2028()
 {
    if (jj_done) return true;
    if (jj_scan_token(TYPE)) return true;
    return false;
  }

 inline bool jj_3_2027()
 {
    if (jj_done) return true;
    if (jj_scan_token(SCOPE_SCHEMA)) return true;
    return false;
  }

 inline bool jj_3R_path_resolved_user_defined_type_name_1281_5_151()
 {
    if (jj_done) return true;
    if (jj_3R_user_defined_type_name_1034_5_892()) return true;
    return false;
  }

 inline bool jj_3_2026()
 {
    if (jj_done) return true;
    if (jj_scan_token(SCOPE_NAME)) return true;
    return false;
  }

 inline bool jj_3_2025()
 {
    if (jj_done) return true;
    if (jj_scan_token(SCOPE_CATALOG)) return true;
    return false;
  }

 inline bool jj_3_2024()
 {
    if (jj_done) return true;
    if (jj_scan_token(SCALE)) return true;
    return false;
  }

 inline bool jj_3_2023()
 {
    if (jj_done) return true;
    if (jj_scan_token(RETURNED_OCTET_LENGTH)) return true;
    return false;
  }

 inline bool jj_3_2022()
 {
    if (jj_done) return true;
    if (jj_scan_token(RETURNED_LENGTH)) return true;
    return false;
  }

 inline bool jj_3_2021()
 {
    if (jj_done) return true;
    if (jj_scan_token(RETURNED_CARDINALITY)) return true;
    return false;
  }

 inline bool jj_3R_referenced_type_1275_5_891()
 {
    if (jj_done) return true;
    if (jj_3R_path_resolved_user_defined_type_name_1281_5_151()) return true;
    return false;
  }

 inline bool jj_3_2020()
 {
    if (jj_done) return true;
    if (jj_scan_token(PRECISION)) return true;
    return false;
  }

 inline bool jj_3_2019()
 {
    if (jj_done) return true;
    if (jj_scan_token(PARAMETER_SPECIFIC_SCHEMA)) return true;
    return false;
  }

 inline bool jj_3_2018()
 {
    if (jj_done) return true;
    if (jj_scan_token(PARAMETER_SPECIFIC_NAME)) return true;
    return false;
  }

 inline bool jj_3_2017()
 {
    if (jj_done) return true;
    if (jj_scan_token(PARAMETER_SPECIFIC_CATALOG)) return true;
    return false;
  }

 inline bool jj_3_2016()
 {
    if (jj_done) return true;
    if (jj_scan_token(PARAMETER_ORDINAL_POSITION)) return true;
    return false;
  }

 inline bool jj_3_491()
 {
    if (jj_done) return true;
    if (jj_3R_with_or_without_time_zone_1238_5_169()) return true;
    return false;
  }

 inline bool jj_3_2015()
 {
    if (jj_done) return true;
    if (jj_scan_token(PARAMETER_MODE)) return true;
    return false;
  }

 inline bool jj_3R_scope_clause_1269_5_171()
 {
    if (jj_done) return true;
    if (jj_scan_token(SCOPE)) return true;
    if (jj_3R_table_name_948_5_382()) return true;
    return false;
  }

 inline bool jj_3_2014()
 {
    if (jj_done) return true;
    if (jj_scan_token(OCTET_LENGTH)) return true;
    return false;
  }

 inline bool jj_3_2013()
 {
    if (jj_done) return true;
    if (jj_scan_token(NULLABLE)) return true;
    return false;
  }

 inline bool jj_3_2012()
 {
    if (jj_done) return true;
    if (jj_scan_token(NAME)) return true;
    return false;
  }

 inline bool jj_3_2011()
 {
    if (jj_done) return true;
    if (jj_scan_token(LEVEL)) return true;
    return false;
  }

 inline bool jj_3_2010()
 {
    if (jj_done) return true;
    if (jj_scan_token(LENGTH)) return true;
    return false;
  }

 inline bool jj_3_2009()
 {
    if (jj_done) return true;
    if (jj_scan_token(KEY_MEMBER)) return true;
    return false;
  }

 inline bool jj_3R_reference_type_1263_5_149()
 {
    if (jj_done) return true;
    if (jj_scan_token(REF)) return true;
    if (jj_scan_token(lparen)) return true;
    if (jj_3R_referenced_type_1275_5_891()) return true;
    if (jj_scan_token(rparen)) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_500()) jj_scanpos = xsp;
    return false;
  }

 inline bool jj_3_2008()
 {
    if (jj_done) return true;
    if (jj_scan_token(INDICATOR)) return true;
    return false;
  }

 inline bool jj_3_2007()
 {
    if (jj_done) return true;
    if (jj_scan_token(DEGREE)) return true;
    return false;
  }

 inline bool jj_3_2006()
 {
    if (jj_done) return true;
    if (jj_scan_token(DATETIME_INTERVAL_PRECISION)) return true;
    return false;
  }

 inline bool jj_3_2005()
 {
    if (jj_done) return true;
    if (jj_scan_token(DATETIME_INTERVAL_CODE)) return true;
    return false;
  }

 inline bool jj_3_2004()
 {
    if (jj_done) return true;
    if (jj_scan_token(DATA)) return true;
    return false;
  }

 inline bool jj_3R_row_type_body_1257_6_890()
 {
    if (jj_done) return true;
    if (jj_scan_token(lparen)) return true;
    if (jj_3R_field_definition_1309_5_170()) return true;
    Token * xsp;
    while (true) {
      xsp = jj_scanpos;
      if (jj_3_499()) { jj_scanpos = xsp; break; }
    }
    if (jj_scan_token(rparen)) return true;
    return false;
  }

 inline bool jj_3_2003()
 {
    if (jj_done) return true;
    if (jj_scan_token(COLLATION_SCHEMA)) return true;
    return false;
  }

 inline bool jj_3_2002()
 {
    if (jj_done) return true;
    if (jj_scan_token(COLLATION_NAME)) return true;
    return false;
  }

 inline bool jj_3_2001()
 {
    if (jj_done) return true;
    if (jj_scan_token(COLLATION_CATALOG)) return true;
    return false;
  }

 inline bool jj_3_2000()
 {
    if (jj_done) return true;
    if (jj_scan_token(CHARACTER_SET_SCHEMA)) return true;
    return false;
  }

 inline bool jj_3_1999()
 {
    if (jj_done) return true;
    if (jj_scan_token(CHARACTER_SET_NAME)) return true;
    return false;
  }

 inline bool jj_3_1998()
 {
    if (jj_done) return true;
    if (jj_scan_token(CHARACTER_SET_CATALOG)) return true;
    return false;
  }

 inline bool jj_3R_descriptor_item_name_7395_5_1017()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1997()) {
    jj_scanpos = xsp;
    if (jj_3_1998()) {
    jj_scanpos = xsp;
    if (jj_3_1999()) {
    jj_scanpos = xsp;
    if (jj_3_2000()) {
    jj_scanpos = xsp;
    if (jj_3_2001()) {
    jj_scanpos = xsp;
    if (jj_3_2002()) {
    jj_scanpos = xsp;
    if (jj_3_2003()) {
    jj_scanpos = xsp;
    if (jj_3_2004()) {
    jj_scanpos = xsp;
    if (jj_3_2005()) {
    jj_scanpos = xsp;
    if (jj_3_2006()) {
    jj_scanpos = xsp;
    if (jj_3_2007()) {
    jj_scanpos = xsp;
    if (jj_3_2008()) {
    jj_scanpos = xsp;
    if (jj_3_2009()) {
    jj_scanpos = xsp;
    if (jj_3_2010()) {
    jj_scanpos = xsp;
    if (jj_3_2011()) {
    jj_scanpos = xsp;
    if (jj_3_2012()) {
    jj_scanpos = xsp;
    if (jj_3_2013()) {
    jj_scanpos = xsp;
    if (jj_3_2014()) {
    jj_scanpos = xsp;
    if (jj_3_2015()) {
    jj_scanpos = xsp;
    if (jj_3_2016()) {
    jj_scanpos = xsp;
    if (jj_3_2017()) {
    jj_scanpos = xsp;
    if (jj_3_2018()) {
    jj_scanpos = xsp;
    if (jj_3_2019()) {
    jj_scanpos = xsp;
    if (jj_3_2020()) {
    jj_scanpos = xsp;
    if (jj_3_2021()) {
    jj_scanpos = xsp;
    if (jj_3_2022()) {
    jj_scanpos = xsp;
    if (jj_3_2023()) {
    jj_scanpos = xsp;
    if (jj_3_2024()) {
    jj_scanpos = xsp;
    if (jj_3_2025()) {
    jj_scanpos = xsp;
    if (jj_3_2026()) {
    jj_scanpos = xsp;
    if (jj_3_2027()) {
    jj_scanpos = xsp;
    if (jj_3_2028()) {
    jj_scanpos = xsp;
    if (jj_3_2029()) {
    jj_scanpos = xsp;
    if (jj_3_2030()) {
    jj_scanpos = xsp;
    if (jj_3_2031()) {
    jj_scanpos = xsp;
    if (jj_3_2032()) {
    jj_scanpos = xsp;
    if (jj_3_2033()) return true;
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    return false;
  }

 inline bool jj_3_1997()
 {
    if (jj_done) return true;
    if (jj_scan_token(CARDINALITY)) return true;
    return false;
  }

 inline bool jj_3R_row_type_1251_5_148()
 {
    if (jj_done) return true;
    if (jj_scan_token(ROW)) return true;
    if (jj_3R_row_type_body_1257_6_890()) return true;
    return false;
  }

 inline bool jj_3R_simple_target_specification_2_7389_5_1015()
 {
    if (jj_done) return true;
    if (jj_3R_simple_target_specification_1452_5_1022()) return true;
    return false;
  }

 inline bool jj_3_1985()
 {
    if (jj_done) return true;
    if (jj_scan_token(WITH)) return true;
    if (jj_scan_token(MAX)) return true;
    if (jj_3R_simple_value_specification_1423_5_220()) return true;
    return false;
  }

 inline bool jj_3_492()
 {
    if (jj_done) return true;
    if (jj_scan_token(lparen)) return true;
    if (jj_scan_token(unsigned_integer)) return true;
    if (jj_scan_token(rparen)) return true;
    return false;
  }

 inline bool jj_3R_interval_type_1245_5_159()
 {
    if (jj_done) return true;
    if (jj_scan_token(INTERVAL)) return true;
    if (jj_3R_interval_qualifier_3874_5_331()) return true;
    return false;
  }

 inline bool jj_3R_simple_target_specification_1_7383_5_1014()
 {
    if (jj_done) return true;
    if (jj_3R_simple_target_specification_1452_5_1022()) return true;
    return false;
  }

 inline bool jj_3_490()
 {
    if (jj_done) return true;
    if (jj_scan_token(lparen)) return true;
    if (jj_scan_token(unsigned_integer)) return true;
    if (jj_scan_token(rparen)) return true;
    return false;
  }

 inline bool jj_3_498()
 {
    if (jj_done) return true;
    if (jj_scan_token(WITHOUT)) return true;
    if (jj_scan_token(TIME)) return true;
    if (jj_scan_token(ZONE)) return true;
    return false;
  }

 inline bool jj_3_497()
 {
    if (jj_done) return true;
    if (jj_scan_token(WITH)) return true;
    if (jj_scan_token(TIME)) return true;
    if (jj_scan_token(ZONE)) return true;
    return false;
  }

 inline bool jj_3R_with_or_without_time_zone_1238_5_169()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_497()) {
    jj_scanpos = xsp;
    if (jj_3_498()) return true;
    }
    return false;
  }

 inline bool jj_3_1988()
 {
    if (jj_done) return true;
    if (jj_scan_token(570)) return true;
    if (jj_3R_get_header_information_7361_5_846()) return true;
    return false;
  }

 inline bool jj_3R_get_item_information_7377_5_847()
 {
    if (jj_done) return true;
    if (jj_3R_simple_target_specification_2_7389_5_1015()) return true;
    if (jj_scan_token(EQUAL)) return true;
    return false;
  }

 inline bool jj_3_487()
 {
    if (jj_done) return true;
    if (jj_3R_char_length_units_1216_5_168()) return true;
    return false;
  }

 inline bool jj_3_493()
 {
    if (jj_done) return true;
    if (jj_3R_with_or_without_time_zone_1238_5_169()) return true;
    return false;
  }

 inline bool jj_3_496()
 {
    if (jj_done) return true;
    if (jj_scan_token(TIMESTAMP)) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_492()) jj_scanpos = xsp;
    xsp = jj_scanpos;
    if (jj_3_493()) jj_scanpos = xsp;
    return false;
  }

 inline bool jj_3_495()
 {
    if (jj_done) return true;
    if (jj_scan_token(TIME)) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_490()) jj_scanpos = xsp;
    xsp = jj_scanpos;
    if (jj_3_491()) jj_scanpos = xsp;
    return false;
  }

 inline bool jj_3_494()
 {
    if (jj_done) return true;
    if (jj_scan_token(DATE)) return true;
    return false;
  }

 inline bool jj_3R_datetime_type_1229_5_158()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_494()) {
    jj_scanpos = xsp;
    if (jj_3_495()) {
    jj_scanpos = xsp;
    if (jj_3_496()) return true;
    }
    }
    return false;
  }

 inline bool jj_3_1996()
 {
    if (jj_done) return true;
    if (jj_scan_token(TOP_LEVEL_COUNT)) return true;
    return false;
  }

 inline bool jj_3_1995()
 {
    if (jj_done) return true;
    if (jj_scan_token(DYNAMIC_FUNCTION_CODE)) return true;
    return false;
  }

 inline bool jj_3_1994()
 {
    if (jj_done) return true;
    if (jj_scan_token(DYNAMIC_FUNCTION)) return true;
    return false;
  }

 inline bool jj_3_1980()
 {
    if (jj_done) return true;
    if (jj_scan_token(FOR)) return true;
    if (jj_3R_character_set_specification_list_6448_5_721()) return true;
    return false;
  }

 inline bool jj_3_1993()
 {
    if (jj_done) return true;
    if (jj_scan_token(KEY_TYPE)) return true;
    return false;
  }

 inline bool jj_3R_header_item_name_7367_5_1016()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1992()) {
    jj_scanpos = xsp;
    if (jj_3_1993()) {
    jj_scanpos = xsp;
    if (jj_3_1994()) {
    jj_scanpos = xsp;
    if (jj_3_1995()) {
    jj_scanpos = xsp;
    if (jj_3_1996()) return true;
    }
    }
    }
    }
    return false;
  }

 inline bool jj_3_484()
 {
    if (jj_done) return true;
    if (jj_scan_token(multiplier)) return true;
    return false;
  }

 inline bool jj_3_1992()
 {
    if (jj_done) return true;
    if (jj_scan_token(COUNT)) return true;
    return false;
  }

 inline bool jj_3R_get_header_information_7361_5_846()
 {
    if (jj_done) return true;
    if (jj_3R_simple_target_specification_1_7383_5_1014()) return true;
    if (jj_scan_token(EQUAL)) return true;
    if (jj_3R_header_item_name_7367_5_1016()) return true;
    return false;
  }

 inline bool jj_3_483()
 {
    if (jj_done) return true;
    if (jj_3R_char_length_units_1216_5_168()) return true;
    return false;
  }

 inline bool jj_3_489()
 {
    if (jj_done) return true;
    if (jj_scan_token(OCTETS)) return true;
    return false;
  }

 inline bool jj_3_488()
 {
    if (jj_done) return true;
    if (jj_scan_token(CHARACTERS)) return true;
    return false;
  }

 inline bool jj_3R_char_length_units_1216_5_168()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_488()) {
    jj_scanpos = xsp;
    if (jj_3_489()) return true;
    }
    return false;
  }

 inline bool jj_3_1989()
 {
    if (jj_done) return true;
    if (jj_scan_token(570)) return true;
    if (jj_3R_get_item_information_7377_5_847()) return true;
    return false;
  }

 inline bool jj_3_467()
 {
    if (jj_done) return true;
    if (jj_scan_token(570)) return true;
    if (jj_scan_token(unsigned_integer)) return true;
    return false;
  }

 inline bool jj_3_465()
 {
    if (jj_done) return true;
    if (jj_scan_token(570)) return true;
    if (jj_scan_token(unsigned_integer)) return true;
    return false;
  }

 inline bool jj_3_1986()
 {
    if (jj_done) return true;
    if (jj_scan_token(SQL)) return true;
    return false;
  }

 inline bool jj_3_1987()
 {
    if (jj_done) return true;
    if (jj_scan_token(SQL)) return true;
    return false;
  }

 inline bool jj_3_469()
 {
    if (jj_done) return true;
    if (jj_scan_token(570)) return true;
    if (jj_scan_token(unsigned_integer)) return true;
    return false;
  }

 inline bool jj_3_1991()
 {
    if (jj_done) return true;
    if (jj_scan_token(VALUE)) return true;
    if (jj_3R_simple_value_specification_1423_5_220()) return true;
    if (jj_3R_get_item_information_7377_5_847()) return true;
    return false;
  }

 inline bool jj_3R_character_large_object_length_1210_5_162()
 {
    if (jj_done) return true;
    if (jj_3R_large_object_length_1203_5_165()) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_487()) jj_scanpos = xsp;
    return false;
  }

 inline bool jj_3_1990()
 {
    if (jj_done) return true;
    if (jj_3R_get_header_information_7361_5_846()) return true;
    return false;
  }

 inline bool jj_3_1981()
 {
    if (jj_done) return true;
    if (jj_scan_token(FOR)) return true;
    if (jj_3R_character_set_specification_list_6448_5_721()) return true;
    return false;
  }

 inline bool jj_3_1984()
 {
    if (jj_done) return true;
    if (jj_scan_token(SQL)) return true;
    return false;
  }

 inline bool jj_3R_get_descriptor_statement_7347_5_810()
 {
    if (jj_done) return true;
    if (jj_scan_token(GET)) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1987()) jj_scanpos = xsp;
    if (jj_scan_token(DESCRIPTOR)) return true;
    if (jj_3R_descriptor_name_1066_5_1008()) return true;
    return false;
  }

 inline bool jj_3_486()
 {
    if (jj_done) return true;
    if (jj_scan_token(large_object_length_token)) return true;
    return false;
  }

 inline bool jj_3_485()
 {
    if (jj_done) return true;
    if (jj_scan_token(unsigned_integer)) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_484()) jj_scanpos = xsp;
    return false;
  }

 inline bool jj_3R_large_object_length_1203_5_165()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_485()) {
    jj_scanpos = xsp;
    if (jj_3_486()) return true;
    }
    return false;
  }

 inline bool jj_3R_deallocate_descriptor_statement_7341_5_808()
 {
    if (jj_done) return true;
    if (jj_scan_token(DEALLOCATE)) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1986()) jj_scanpos = xsp;
    if (jj_scan_token(DESCRIPTOR)) return true;
    if (jj_3R_descriptor_name_1066_5_1008()) return true;
    return false;
  }

 inline bool jj_3_478()
 {
    if (jj_done) return true;
    if (jj_scan_token(lparen)) return true;
    if (jj_scan_token(unsigned_integer)) return true;
    if (jj_scan_token(rparen)) return true;
    return false;
  }

 inline bool jj_3R_character_length_1197_5_160()
 {
    if (jj_done) return true;
    if (jj_scan_token(unsigned_integer)) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_483()) jj_scanpos = xsp;
    return false;
  }

 inline bool jj_3_479()
 {
    if (jj_done) return true;
    if (jj_scan_token(PRECISION)) return true;
    return false;
  }

 inline bool jj_3R_allocate_descriptor_statement_7335_5_807()
 {
    if (jj_done) return true;
    if (jj_scan_token(ALLOCATE)) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1984()) jj_scanpos = xsp;
    if (jj_scan_token(DESCRIPTOR)) return true;
    if (jj_3R_descriptor_name_1066_5_1008()) return true;
    return false;
  }

 inline bool jj_3_459()
 {
    if (jj_done) return true;
    if (jj_scan_token(lparen)) return true;
    if (jj_3R_large_object_length_1203_5_165()) return true;
    if (jj_scan_token(rparen)) return true;
    return false;
  }

 inline bool jj_3_482()
 {
    if (jj_done) return true;
    if (jj_scan_token(DOUBLE)) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_479()) jj_scanpos = xsp;
    return false;
  }

 inline bool jj_3_468()
 {
    if (jj_done) return true;
    if (jj_scan_token(lparen)) return true;
    if (jj_scan_token(unsigned_integer)) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_467()) jj_scanpos = xsp;
    if (jj_scan_token(rparen)) return true;
    return false;
  }

 inline bool jj_3_481()
 {
    if (jj_done) return true;
    if (jj_scan_token(REAL)) return true;
    return false;
  }

 inline bool jj_3_466()
 {
    if (jj_done) return true;
    if (jj_scan_token(lparen)) return true;
    if (jj_scan_token(unsigned_integer)) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_465()) jj_scanpos = xsp;
    if (jj_scan_token(rparen)) return true;
    return false;
  }

 inline bool jj_3_480()
 {
    if (jj_done) return true;
    if (jj_scan_token(FLOAT)) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_478()) jj_scanpos = xsp;
    return false;
  }

 inline bool jj_3R_approximate_numeric_type_1188_5_167()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_480()) {
    jj_scanpos = xsp;
    if (jj_3_481()) {
    jj_scanpos = xsp;
    if (jj_3_482()) return true;
    }
    }
    return false;
  }

 inline bool jj_3R_collation_specification_7329_5_845()
 {
    if (jj_done) return true;
    if (jj_3R_value_specification_1379_5_844()) return true;
    return false;
  }

 inline bool jj_3_447()
 {
    if (jj_done) return true;
    if (jj_scan_token(lparen)) return true;
    if (jj_3R_character_large_object_length_1210_5_162()) return true;
    if (jj_scan_token(rparen)) return true;
    return false;
  }

 inline bool jj_3_470()
 {
    if (jj_done) return true;
    if (jj_scan_token(lparen)) return true;
    if (jj_scan_token(unsigned_integer)) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_469()) jj_scanpos = xsp;
    if (jj_scan_token(rparen)) return true;
    return false;
  }

 inline bool jj_3_477()
 {
    if (jj_done) return true;
    if (jj_scan_token(BIGINT)) return true;
    return false;
  }

 inline bool jj_3_476()
 {
    if (jj_done) return true;
    if (jj_scan_token(INT)) return true;
    return false;
  }

 inline bool jj_3_475()
 {
    if (jj_done) return true;
    if (jj_scan_token(INTEGER)) return true;
    return false;
  }

 inline bool jj_3_1983()
 {
    if (jj_done) return true;
    if (jj_scan_token(SET)) return true;
    if (jj_scan_token(NO)) return true;
    if (jj_scan_token(COLLATION)) return true;
    return false;
  }

 inline bool jj_3R_set_session_collation_statement_7322_5_793()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1982()) {
    jj_scanpos = xsp;
    if (jj_3_1983()) return true;
    }
    return false;
  }

 inline bool jj_3_474()
 {
    if (jj_done) return true;
    if (jj_scan_token(SMALLINT)) return true;
    return false;
  }

 inline bool jj_3_1982()
 {
    if (jj_done) return true;
    if (jj_scan_token(SET)) return true;
    if (jj_scan_token(COLLATION)) return true;
    if (jj_3R_collation_specification_7329_5_845()) return true;
    return false;
  }

 inline bool jj_3_473()
 {
    if (jj_done) return true;
    if (jj_scan_token(DEC)) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_470()) jj_scanpos = xsp;
    return false;
  }

 inline bool jj_3_472()
 {
    if (jj_done) return true;
    if (jj_scan_token(DECIMAL)) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_468()) jj_scanpos = xsp;
    return false;
  }

 inline bool jj_3_471()
 {
    if (jj_done) return true;
    if (jj_scan_token(NUMERIC)) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_466()) jj_scanpos = xsp;
    return false;
  }

 inline bool jj_3R_exact_numeric_type_1176_5_166()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_471()) {
    jj_scanpos = xsp;
    if (jj_3_472()) {
    jj_scanpos = xsp;
    if (jj_3_473()) {
    jj_scanpos = xsp;
    if (jj_3_474()) {
    jj_scanpos = xsp;
    if (jj_3_475()) {
    jj_scanpos = xsp;
    if (jj_3_476()) {
    jj_scanpos = xsp;
    if (jj_3_477()) return true;
    }
    }
    }
    }
    }
    }
    return false;
  }

 inline bool jj_3_1979()
 {
    if (jj_done) return true;
    if (jj_scan_token(TRANSFORM)) return true;
    if (jj_scan_token(GROUP)) return true;
    if (jj_scan_token(FOR)) return true;
    return false;
  }

 inline bool jj_3R_transform_group_characteristic_7315_5_1004()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1978()) {
    jj_scanpos = xsp;
    if (jj_3_1979()) return true;
    }
    return false;
  }

 inline bool jj_3_448()
 {
    if (jj_done) return true;
    if (jj_scan_token(lparen)) return true;
    if (jj_3R_character_large_object_length_1210_5_162()) return true;
    if (jj_scan_token(rparen)) return true;
    return false;
  }

 inline bool jj_3_460()
 {
    if (jj_done) return true;
    if (jj_scan_token(lparen)) return true;
    if (jj_3R_large_object_length_1203_5_165()) return true;
    if (jj_scan_token(rparen)) return true;
    return false;
  }

 inline bool jj_3_1978()
 {
    if (jj_done) return true;
    if (jj_scan_token(DEFAULT_)) return true;
    if (jj_scan_token(TRANSFORM)) return true;
    if (jj_scan_token(GROUP)) return true;
    return false;
  }

 inline bool jj_3_464()
 {
    if (jj_done) return true;
    if (jj_3R_approximate_numeric_type_1188_5_167()) return true;
    return false;
  }

 inline bool jj_3_463()
 {
    if (jj_done) return true;
    if (jj_3R_exact_numeric_type_1176_5_166()) return true;
    return false;
  }

 inline bool jj_3R_numeric_type_1169_5_157()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_463()) {
    jj_scanpos = xsp;
    if (jj_3_464()) return true;
    }
    return false;
  }

 inline bool jj_3R_set_transform_group_statement_7309_5_792()
 {
    if (jj_done) return true;
    if (jj_scan_token(SET)) return true;
    if (jj_3R_transform_group_characteristic_7315_5_1004()) return true;
    return false;
  }

 inline bool jj_3_453()
 {
    if (jj_done) return true;
    if (jj_scan_token(lparen)) return true;
    if (jj_scan_token(unsigned_integer)) return true;
    if (jj_scan_token(rparen)) return true;
    return false;
  }

 inline bool jj_3_462()
 {
    if (jj_done) return true;
    if (jj_scan_token(BLOB)) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_460()) jj_scanpos = xsp;
    return false;
  }

 inline bool jj_3_461()
 {
    if (jj_done) return true;
    if (jj_scan_token(BINARY)) return true;
    if (jj_scan_token(LARGE)) return true;
    if (jj_scan_token(OBJECT)) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_459()) jj_scanpos = xsp;
    return false;
  }

 inline bool jj_3R_binary_large_object_string_type_1162_5_164()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_461()) {
    jj_scanpos = xsp;
    if (jj_3_462()) return true;
    }
    return false;
  }

 inline bool jj_3R_SQL_path_characteristic_7303_5_1003()
 {
    if (jj_done) return true;
    if (jj_scan_token(PATH)) return true;
    if (jj_3R_value_specification_1379_5_844()) return true;
    return false;
  }

 inline bool jj_3_437()
 {
    if (jj_done) return true;
    if (jj_scan_token(lparen)) return true;
    if (jj_3R_character_length_1197_5_160()) return true;
    if (jj_scan_token(rparen)) return true;
    return false;
  }

 inline bool jj_3_449()
 {
    if (jj_done) return true;
    if (jj_scan_token(lparen)) return true;
    if (jj_3R_character_large_object_length_1210_5_162()) return true;
    if (jj_scan_token(rparen)) return true;
    return false;
  }

 inline bool jj_3_458()
 {
    if (jj_done) return true;
    if (jj_3R_binary_large_object_string_type_1162_5_164()) return true;
    return false;
  }

 inline bool jj_3_431()
 {
    if (jj_done) return true;
    if (jj_scan_token(lparen)) return true;
    if (jj_3R_character_large_object_length_1210_5_162()) return true;
    if (jj_scan_token(rparen)) return true;
    return false;
  }

 inline bool jj_3_457()
 {
    if (jj_done) return true;
    if (jj_scan_token(546)) return true;
    return false;
  }

 inline bool jj_3R_set_path_statement_7297_5_791()
 {
    if (jj_done) return true;
    if (jj_scan_token(SET)) return true;
    if (jj_3R_SQL_path_characteristic_7303_5_1003()) return true;
    return false;
  }

 inline bool jj_3_456()
 {
    if (jj_done) return true;
    if (jj_scan_token(VARBINARY)) return true;
    if (jj_scan_token(lparen)) return true;
    if (jj_scan_token(unsigned_integer)) return true;
    if (jj_scan_token(rparen)) return true;
    return false;
  }

 inline bool jj_3_438()
 {
    if (jj_done) return true;
    if (jj_scan_token(lparen)) return true;
    if (jj_3R_character_length_1197_5_160()) return true;
    if (jj_scan_token(rparen)) return true;
    return false;
  }

 inline bool jj_3_455()
 {
    if (jj_done) return true;
    if (jj_scan_token(BINARY)) return true;
    if (jj_scan_token(VARYING)) return true;
    if (jj_scan_token(lparen)) return true;
    if (jj_scan_token(unsigned_integer)) return true;
    if (jj_scan_token(rparen)) return true;
    return false;
  }

 inline bool jj_3_454()
 {
    if (jj_done) return true;
    if (jj_scan_token(BINARY)) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_453()) jj_scanpos = xsp;
    return false;
  }

 inline bool jj_3R_binary_string_type_1152_5_156()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_454()) {
    jj_scanpos = xsp;
    if (jj_3_455()) {
    jj_scanpos = xsp;
    if (jj_3_456()) {
    jj_scanpos = xsp;
    if (jj_3_457()) {
    jj_scanpos = xsp;
    if (jj_3_458()) return true;
    }
    }
    }
    }
    return false;
  }

 inline bool jj_3_432()
 {
    if (jj_done) return true;
    if (jj_scan_token(lparen)) return true;
    if (jj_3R_character_large_object_length_1210_5_162()) return true;
    if (jj_scan_token(rparen)) return true;
    return false;
  }

 inline bool jj_3R_character_set_name_characteristic_7291_5_1002()
 {
    if (jj_done) return true;
    if (jj_scan_token(NAMES)) return true;
    if (jj_3R_value_specification_1379_5_844()) return true;
    return false;
  }

 inline bool jj_3_452()
 {
    if (jj_done) return true;
    if (jj_scan_token(NCLOB)) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_449()) jj_scanpos = xsp;
    return false;
  }

 inline bool jj_3_451()
 {
    if (jj_done) return true;
    if (jj_scan_token(NCHAR)) return true;
    if (jj_scan_token(LARGE)) return true;
    if (jj_scan_token(OBJECT)) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_448()) jj_scanpos = xsp;
    return false;
  }

 inline bool jj_3_439()
 {
    if (jj_done) return true;
    if (jj_scan_token(lparen)) return true;
    if (jj_3R_character_length_1197_5_160()) return true;
    if (jj_scan_token(rparen)) return true;
    return false;
  }

 inline bool jj_3_450()
 {
    if (jj_done) return true;
    if (jj_scan_token(NATIONAL)) return true;
    if (jj_scan_token(CHARACTER)) return true;
    if (jj_scan_token(LARGE)) return true;
    if (jj_scan_token(OBJECT)) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_447()) jj_scanpos = xsp;
    return false;
  }

 inline bool jj_3R_national_character_large_object_type_1144_5_163()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_450()) {
    jj_scanpos = xsp;
    if (jj_3_451()) {
    jj_scanpos = xsp;
    if (jj_3_452()) return true;
    }
    }
    return false;
  }

 inline bool jj_3R_set_names_statement_7285_5_790()
 {
    if (jj_done) return true;
    if (jj_scan_token(SET)) return true;
    if (jj_3R_character_set_name_characteristic_7291_5_1002()) return true;
    return false;
  }

 inline bool jj_3_446()
 {
    if (jj_done) return true;
    if (jj_3R_national_character_large_object_type_1144_5_163()) return true;
    return false;
  }

 inline bool jj_3_414()
 {
    if (jj_done) return true;
    if (jj_3R_collate_clause_4076_5_153()) return true;
    return false;
  }

 inline bool jj_3_445()
 {
    if (jj_done) return true;
    if (jj_scan_token(NCHAR)) return true;
    if (jj_scan_token(VARYING)) return true;
    if (jj_scan_token(lparen)) return true;
    if (jj_3R_character_length_1197_5_160()) return true;
    if (jj_scan_token(rparen)) return true;
    return false;
  }

 inline bool jj_3R_schema_name_characteristic_7279_5_1001()
 {
    if (jj_done) return true;
    if (jj_scan_token(SCHEMA)) return true;
    if (jj_3R_value_specification_1379_5_844()) return true;
    return false;
  }

 inline bool jj_3_444()
 {
    if (jj_done) return true;
    if (jj_scan_token(NATIONAL)) return true;
    if (jj_scan_token(CHAR)) return true;
    if (jj_scan_token(VARYING)) return true;
    if (jj_scan_token(lparen)) return true;
    if (jj_3R_character_length_1197_5_160()) return true;
    if (jj_scan_token(rparen)) return true;
    return false;
  }

 inline bool jj_3_433()
 {
    if (jj_done) return true;
    if (jj_scan_token(lparen)) return true;
    if (jj_3R_character_large_object_length_1210_5_162()) return true;
    if (jj_scan_token(rparen)) return true;
    return false;
  }

 inline bool jj_3_443()
 {
    if (jj_done) return true;
    if (jj_scan_token(NATIONAL)) return true;
    if (jj_scan_token(CHARACTER)) return true;
    if (jj_scan_token(VARYING)) return true;
    if (jj_scan_token(lparen)) return true;
    if (jj_3R_character_length_1197_5_160()) return true;
    if (jj_scan_token(rparen)) return true;
    return false;
  }

 inline bool jj_3_442()
 {
    if (jj_done) return true;
    if (jj_scan_token(NCHAR)) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_439()) jj_scanpos = xsp;
    return false;
  }

 inline bool jj_3_441()
 {
    if (jj_done) return true;
    if (jj_scan_token(NATIONAL)) return true;
    if (jj_scan_token(CHAR)) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_438()) jj_scanpos = xsp;
    return false;
  }

 inline bool jj_3_440()
 {
    if (jj_done) return true;
    if (jj_scan_token(NATIONAL)) return true;
    if (jj_scan_token(CHARACTER)) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_437()) jj_scanpos = xsp;
    return false;
  }

 inline bool jj_3R_national_character_string_type_1132_5_155()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_440()) {
    jj_scanpos = xsp;
    if (jj_3_441()) {
    jj_scanpos = xsp;
    if (jj_3_442()) {
    jj_scanpos = xsp;
    if (jj_3_443()) {
    jj_scanpos = xsp;
    if (jj_3_444()) {
    jj_scanpos = xsp;
    if (jj_3_445()) {
    jj_scanpos = xsp;
    if (jj_3_446()) return true;
    }
    }
    }
    }
    }
    }
    return false;
  }

 inline bool jj_3R_set_schema_statement_7273_5_789()
 {
    if (jj_done) return true;
    if (jj_scan_token(SET)) return true;
    if (jj_3R_schema_name_characteristic_7279_5_1001()) return true;
    return false;
  }

 inline bool jj_3_424()
 {
    if (jj_done) return true;
    if (jj_scan_token(lparen)) return true;
    if (jj_3R_character_length_1197_5_160()) return true;
    if (jj_scan_token(rparen)) return true;
    return false;
  }

 inline bool jj_3_422()
 {
    if (jj_done) return true;
    if (jj_scan_token(lparen)) return true;
    if (jj_3R_character_length_1197_5_160()) return true;
    if (jj_scan_token(rparen)) return true;
    return false;
  }

 inline bool jj_3_412()
 {
    if (jj_done) return true;
    if (jj_scan_token(CHARACTER)) return true;
    if (jj_scan_token(SET)) return true;
    if (jj_3R_character_set_specification_4015_5_133()) return true;
    return false;
  }

 inline bool jj_3_436()
 {
    if (jj_done) return true;
    if (jj_scan_token(CLOB)) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_433()) jj_scanpos = xsp;
    return false;
  }

 inline bool jj_3_435()
 {
    if (jj_done) return true;
    if (jj_scan_token(CHAR)) return true;
    if (jj_scan_token(LARGE)) return true;
    if (jj_scan_token(OBJECT)) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_432()) jj_scanpos = xsp;
    return false;
  }

 inline bool jj_3R_catalog_name_characteristic_7267_5_1000()
 {
    if (jj_done) return true;
    if (jj_scan_token(CATALOG)) return true;
    if (jj_3R_value_specification_1379_5_844()) return true;
    return false;
  }

 inline bool jj_3_434()
 {
    if (jj_done) return true;
    if (jj_scan_token(CHARACTER)) return true;
    if (jj_scan_token(LARGE)) return true;
    if (jj_scan_token(OBJECT)) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_431()) jj_scanpos = xsp;
    return false;
  }

 inline bool jj_3R_character_large_object_type_1124_5_161()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_434()) {
    jj_scanpos = xsp;
    if (jj_3_435()) {
    jj_scanpos = xsp;
    if (jj_3_436()) return true;
    }
    }
    return false;
  }

 inline bool jj_3_423()
 {
    if (jj_done) return true;
    if (jj_scan_token(lparen)) return true;
    if (jj_3R_character_length_1197_5_160()) return true;
    if (jj_scan_token(rparen)) return true;
    return false;
  }

 inline bool jj_3R_set_catalog_statement_7261_5_788()
 {
    if (jj_done) return true;
    if (jj_scan_token(SET)) return true;
    if (jj_3R_catalog_name_characteristic_7267_5_1000()) return true;
    return false;
  }

 inline bool jj_3_430()
 {
    if (jj_done) return true;
    if (jj_3R_character_large_object_type_1124_5_161()) return true;
    return false;
  }

 inline bool jj_3_429()
 {
    if (jj_done) return true;
    if (jj_scan_token(VARCHAR)) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_424()) jj_scanpos = xsp;
    return false;
  }

 inline bool jj_3_428()
 {
    if (jj_done) return true;
    if (jj_scan_token(CHAR)) return true;
    if (jj_scan_token(VARYING)) return true;
    if (jj_scan_token(lparen)) return true;
    if (jj_3R_character_length_1197_5_160()) return true;
    if (jj_scan_token(rparen)) return true;
    return false;
  }

 inline bool jj_3_427()
 {
    if (jj_done) return true;
    if (jj_scan_token(CHARACTER)) return true;
    if (jj_scan_token(VARYING)) return true;
    if (jj_scan_token(lparen)) return true;
    if (jj_3R_character_length_1197_5_160()) return true;
    if (jj_scan_token(rparen)) return true;
    return false;
  }

 inline bool jj_3_1973()
 {
    if (jj_done) return true;
    if (jj_scan_token(570)) return true;
    if (jj_3R_transaction_mode_7080_5_837()) return true;
    return false;
  }

 inline bool jj_3_426()
 {
    if (jj_done) return true;
    if (jj_scan_token(CHAR)) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_423()) jj_scanpos = xsp;
    return false;
  }

 inline bool jj_3_425()
 {
    if (jj_done) return true;
    if (jj_scan_token(CHARACTER)) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_422()) jj_scanpos = xsp;
    return false;
  }

 inline bool jj_3R_character_string_type_1113_5_154()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_425()) {
    jj_scanpos = xsp;
    if (jj_3_426()) {
    jj_scanpos = xsp;
    if (jj_3_427()) {
    jj_scanpos = xsp;
    if (jj_3_428()) {
    jj_scanpos = xsp;
    if (jj_3_429()) {
    jj_scanpos = xsp;
    if (jj_3_430()) return true;
    }
    }
    }
    }
    }
    return false;
  }

 inline bool jj_3_1977()
 {
    if (jj_done) return true;
    if (jj_scan_token(LOCAL)) return true;
    return false;
  }

 inline bool jj_3_1976()
 {
    if (jj_done) return true;
    if (jj_3R_interval_value_expression_2449_5_264()) return true;
    return false;
  }

 inline bool jj_3_421()
 {
    if (jj_done) return true;
    if (jj_3R_interval_type_1245_5_159()) return true;
    return false;
  }

 inline bool jj_3_420()
 {
    if (jj_done) return true;
    if (jj_3R_datetime_type_1229_5_158()) return true;
    return false;
  }

 inline bool jj_3R_set_local_time_zone_statement_7248_5_786()
 {
    if (jj_done) return true;
    if (jj_scan_token(SET)) return true;
    if (jj_scan_token(TIME)) return true;
    if (jj_scan_token(ZONE)) return true;
    return false;
  }

 inline bool jj_3_419()
 {
    if (jj_done) return true;
    if (jj_scan_token(363)) return true;
    return false;
  }

 inline bool jj_3_418()
 {
    if (jj_done) return true;
    if (jj_3R_numeric_type_1169_5_157()) return true;
    return false;
  }

 inline bool jj_3_413()
 {
    if (jj_done) return true;
    if (jj_3R_collate_clause_4076_5_153()) return true;
    return false;
  }

 inline bool jj_3_417()
 {
    if (jj_done) return true;
    if (jj_3R_binary_string_type_1152_5_156()) return true;
    return false;
  }

 inline bool jj_3_416()
 {
    if (jj_done) return true;
    if (jj_3R_national_character_string_type_1132_5_155()) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_414()) jj_scanpos = xsp;
    return false;
  }

 inline bool jj_3_415()
 {
    if (jj_done) return true;
    if (jj_3R_character_string_type_1113_5_154()) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_412()) jj_scanpos = xsp;
    xsp = jj_scanpos;
    if (jj_3_413()) jj_scanpos = xsp;
    return false;
  }

 inline bool jj_3R_predefined_type_1100_5_147()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_415()) {
    jj_scanpos = xsp;
    if (jj_3_416()) {
    jj_scanpos = xsp;
    if (jj_3_417()) {
    jj_scanpos = xsp;
    if (jj_3_418()) {
    jj_scanpos = xsp;
    if (jj_3_419()) {
    jj_scanpos = xsp;
    if (jj_3_420()) {
    jj_scanpos = xsp;
    if (jj_3_421()) return true;
    }
    }
    }
    }
    }
    }
    return false;
  }

 inline bool jj_3_1975()
 {
    if (jj_done) return true;
    if (jj_scan_token(NONE)) return true;
    return false;
  }

 inline bool jj_3R_role_specification_7241_5_999()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1974()) {
    jj_scanpos = xsp;
    if (jj_3_1975()) return true;
    }
    return false;
  }

 inline bool jj_3_1974()
 {
    if (jj_done) return true;
    if (jj_3R_value_specification_1379_5_844()) return true;
    return false;
  }

 inline bool jj_3_1964()
 {
    if (jj_done) return true;
    if (jj_scan_token(USER)) return true;
    if (jj_3R_simple_value_specification_1423_5_220()) return true;
    return false;
  }

 inline bool jj_3_1972()
 {
    if (jj_done) return true;
    if (jj_scan_token(570)) return true;
    if (jj_3R_session_characteristic_7217_5_843()) return true;
    return false;
  }

 inline bool jj_3_411()
 {
    if (jj_done) return true;
    if (jj_3R_collection_type_1287_5_152()) return true;
    return false;
  }

 inline bool jj_3_410()
 {
    if (jj_done) return true;
    if (jj_3R_path_resolved_user_defined_type_name_1281_5_151()) return true;
    return false;
  }

 inline bool jj_3R_set_role_statement_7235_5_785()
 {
    if (jj_done) return true;
    if (jj_scan_token(SET)) return true;
    if (jj_scan_token(ROLE)) return true;
    if (jj_3R_role_specification_7241_5_999()) return true;
    return false;
  }

 inline bool jj_3_409()
 {
    if (jj_done) return true;
    if (jj_3R_presto_generic_type_7956_5_150()) return true;
    return false;
  }

 inline bool jj_3_408()
 {
    if (jj_done) return true;
    if (jj_3R_reference_type_1263_5_149()) return true;
    return false;
  }

 inline bool jj_3_407()
 {
    if (jj_done) return true;
    if (jj_3R_row_type_1251_5_148()) return true;
    return false;
  }

 inline bool jj_3_406()
 {
    if (jj_done) return true;
    if (jj_3R_predefined_type_1100_5_147()) return true;
    return false;
  }

 inline bool jj_3R_set_session_user_identifier_statement_7229_5_784()
 {
    if (jj_done) return true;
    if (jj_scan_token(SET)) return true;
    if (jj_scan_token(SESSION)) return true;
    if (jj_scan_token(AUTHORIZATION)) return true;
    return false;
  }

 inline bool jj_3R_data_type_1086_3_251()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_406()) {
    jj_scanpos = xsp;
    if (jj_3_407()) {
    jj_scanpos = xsp;
    if (jj_3_408()) {
    jj_scanpos = xsp;
    if (jj_3_409()) {
    jj_scanpos = xsp;
    if (jj_3_410()) return true;
    }
    }
    }
    }
    xsp = jj_scanpos;
    if (jj_3_411()) jj_scanpos = xsp;
    return false;
  }

 inline bool jj_3R_session_transaction_characteristics_7223_5_1013()
 {
    if (jj_done) return true;
    if (jj_scan_token(TRANSACTION)) return true;
    if (jj_3R_transaction_mode_7080_5_837()) return true;
    return false;
  }

 inline bool jj_3_405()
 {
    if (jj_done) return true;
    if (jj_scan_token(LOCAL)) return true;
    return false;
  }

 inline bool jj_3_404()
 {
    if (jj_done) return true;
    if (jj_scan_token(GLOBAL)) return true;
    return false;
  }

 inline bool jj_3R_scope_option_1079_5_143()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_404()) {
    jj_scanpos = xsp;
    if (jj_3_405()) return true;
    }
    return false;
  }

 inline bool jj_3_403()
 {
    if (jj_done) return true;
    if (jj_3R_scope_option_1079_5_143()) return true;
    return false;
  }

 inline bool jj_3R_session_characteristic_7217_5_843()
 {
    if (jj_done) return true;
    if (jj_3R_session_transaction_characteristics_7223_5_1013()) return true;
    return false;
  }

 inline bool jj_3R_extended_descriptor_name_1073_5_146()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_403()) jj_scanpos = xsp;
    if (jj_3R_simple_value_specification_1423_5_220()) return true;
    return false;
  }

 inline bool jj_3_402()
 {
    if (jj_done) return true;
    if (jj_3R_extended_descriptor_name_1073_5_146()) return true;
    return false;
  }

 inline bool jj_3R_descriptor_name_1066_5_1008()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_401()) {
    jj_scanpos = xsp;
    if (jj_3_402()) return true;
    }
    return false;
  }

 inline bool jj_3_401()
 {
    if (jj_done) return true;
    if (jj_3R_identifier_928_3_141()) return true;
    return false;
  }

 inline bool jj_3R_set_session_characteristics_statement_7205_5_787()
 {
    if (jj_done) return true;
    if (jj_scan_token(SET)) return true;
    if (jj_scan_token(SESSION)) return true;
    if (jj_scan_token(CHARACTERISTICS)) return true;
    return false;
  }

 inline bool jj_3_400()
 {
    if (jj_done) return true;
    if (jj_3R_scope_option_1079_5_143()) return true;
    return false;
  }

 inline bool jj_3_1962()
 {
    if (jj_done) return true;
    if (jj_3R_savepoint_clause_7159_5_841()) return true;
    return false;
  }

 inline bool jj_3R_extended_cursor_name_1060_5_145()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_400()) jj_scanpos = xsp;
    if (jj_3R_simple_value_specification_1423_5_220()) return true;
    return false;
  }

 inline bool jj_3_1963()
 {
    if (jj_done) return true;
    if (jj_scan_token(AS)) return true;
    if (jj_3R_simple_value_specification_1423_5_220()) return true;
    return false;
  }

 inline bool jj_3_1971()
 {
    if (jj_done) return true;
    if (jj_scan_token(CURRENT)) return true;
    return false;
  }

 inline bool jj_3_1970()
 {
    if (jj_done) return true;
    if (jj_scan_token(ALL)) return true;
    return false;
  }

 inline bool jj_3R_disconnect_object_7197_5_998()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1969()) {
    jj_scanpos = xsp;
    if (jj_3_1970()) {
    jj_scanpos = xsp;
    if (jj_3_1971()) return true;
    }
    }
    return false;
  }

 inline bool jj_3_399()
 {
    if (jj_done) return true;
    if (jj_3R_extended_cursor_name_1060_5_145()) return true;
    return false;
  }

 inline bool jj_3_1969()
 {
    if (jj_done) return true;
    if (jj_3R_connection_object_7184_5_842()) return true;
    return false;
  }

 inline bool jj_3R_dynamic_cursor_name_1053_5_1006()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_398()) {
    jj_scanpos = xsp;
    if (jj_3_399()) return true;
    }
    return false;
  }

 inline bool jj_3_398()
 {
    if (jj_done) return true;
    if (jj_3R_cursor_name_995_5_144()) return true;
    return false;
  }

 inline bool jj_3_397()
 {
    if (jj_done) return true;
    if (jj_3R_scope_option_1079_5_143()) return true;
    return false;
  }

 inline bool jj_3R_disconnect_statement_7191_5_783()
 {
    if (jj_done) return true;
    if (jj_scan_token(DISCONNECT)) return true;
    if (jj_3R_disconnect_object_7197_5_998()) return true;
    return false;
  }

 inline bool jj_3R_extended_identifier_1047_5_142()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_397()) jj_scanpos = xsp;
    if (jj_3R_simple_value_specification_1423_5_220()) return true;
    return false;
  }

 inline bool jj_3_1960()
 {
    if (jj_done) return true;
    if (jj_scan_token(NO)) return true;
    return false;
  }

 inline bool jj_3_1968()
 {
    if (jj_done) return true;
    if (jj_3R_simple_value_specification_1423_5_220()) return true;
    return false;
  }

 inline bool jj_3R_connection_object_7184_5_842()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1967()) {
    jj_scanpos = xsp;
    if (jj_3_1968()) return true;
    }
    return false;
  }

 inline bool jj_3_396()
 {
    if (jj_done) return true;
    if (jj_3R_extended_identifier_1047_5_142()) return true;
    return false;
  }

 inline bool jj_3_1967()
 {
    if (jj_done) return true;
    if (jj_scan_token(DEFAULT_)) return true;
    return false;
  }

 inline bool jj_3R_SQL_identifier_1040_5_865()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_395()) {
    jj_scanpos = xsp;
    if (jj_3_396()) return true;
    }
    return false;
  }

 inline bool jj_3_395()
 {
    if (jj_done) return true;
    if (jj_3R_identifier_928_3_141()) return true;
    return false;
  }

 inline bool jj_3R_set_connection_statement_7178_5_782()
 {
    if (jj_done) return true;
    if (jj_scan_token(SET)) return true;
    if (jj_scan_token(CONNECTION)) return true;
    if (jj_3R_connection_object_7184_5_842()) return true;
    return false;
  }

 inline bool jj_3R_user_defined_type_name_1034_5_892()
 {
    if (jj_done) return true;
    if (jj_3R_identifier_chain_1496_5_207()) return true;
    return false;
  }

 inline bool jj_3_1957()
 {
    if (jj_done) return true;
    if (jj_scan_token(NO)) return true;
    return false;
  }

 inline bool jj_3_1961()
 {
    if (jj_done) return true;
    if (jj_scan_token(AND)) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1960()) jj_scanpos = xsp;
    if (jj_scan_token(CHAIN)) return true;
    return false;
  }

 inline bool jj_3_1952()
 {
    if (jj_done) return true;
    if (jj_scan_token(IMMEDIATE)) return true;
    return false;
  }

 inline bool jj_3_1966()
 {
    if (jj_done) return true;
    if (jj_scan_token(DEFAULT_)) return true;
    return false;
  }

 inline bool jj_3R_connection_target_7171_5_997()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1965()) {
    jj_scanpos = xsp;
    if (jj_3_1966()) return true;
    }
    return false;
  }

 inline bool jj_3_1965()
 {
    if (jj_done) return true;
    if (jj_3R_simple_value_specification_1423_5_220()) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1963()) jj_scanpos = xsp;
    xsp = jj_scanpos;
    if (jj_3_1964()) jj_scanpos = xsp;
    return false;
  }

 inline bool jj_3R_schema_resolved_user_defined_type_name_1026_5_479()
 {
    if (jj_done) return true;
    if (jj_3R_user_defined_type_name_1034_5_892()) return true;
    return false;
  }

 inline bool jj_3_1958()
 {
    if (jj_done) return true;
    if (jj_scan_token(AND)) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1957()) jj_scanpos = xsp;
    if (jj_scan_token(CHAIN)) return true;
    return false;
  }

 inline bool jj_3_1959()
 {
    if (jj_done) return true;
    if (jj_scan_token(WORK)) return true;
    return false;
  }

 inline bool jj_3R_connect_statement_7165_5_781()
 {
    if (jj_done) return true;
    if (jj_scan_token(CONNECT)) return true;
    if (jj_scan_token(TO)) return true;
    if (jj_3R_connection_target_7171_5_997()) return true;
    return false;
  }

 inline bool jj_3_394()
 {
    if (jj_done) return true;
    if (jj_3R_schema_name_956_5_140()) return true;
    if (jj_scan_token(569)) return true;
    return false;
  }

 inline bool jj_3R_character_set_name_1020_5_707()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_394()) jj_scanpos = xsp;
    if (jj_scan_token(SQL_language_identifier)) return true;
    return false;
  }

 inline bool jj_3_1951()
 {
    if (jj_done) return true;
    if (jj_scan_token(DEFERRED)) return true;
    return false;
  }

 inline bool jj_3R_savepoint_clause_7159_5_841()
 {
    if (jj_done) return true;
    if (jj_scan_token(TO)) return true;
    if (jj_scan_token(SAVEPOINT)) return true;
    if (jj_3R_savepoint_specifier_7135_5_996()) return true;
    return false;
  }

 inline bool jj_3_393()
 {
    if (jj_done) return true;
    if (jj_3R_character_string_literal_866_3_128()) return true;
    return false;
  }

 inline bool jj_3_1956()
 {
    if (jj_done) return true;
    if (jj_scan_token(WORK)) return true;
    return false;
  }

 inline bool jj_3R_external_routine_name_1013_5_668()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_392()) {
    jj_scanpos = xsp;
    if (jj_3_393()) return true;
    }
    return false;
  }

 inline bool jj_3_392()
 {
    if (jj_done) return true;
    if (jj_3R_identifier_928_3_141()) return true;
    return false;
  }

 inline bool jj_3R_rollback_statement_7153_5_780()
 {
    if (jj_done) return true;
    if (jj_scan_token(ROLLBACK)) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1959()) jj_scanpos = xsp;
    xsp = jj_scanpos;
    if (jj_3_1961()) jj_scanpos = xsp;
    xsp = jj_scanpos;
    if (jj_3_1962()) jj_scanpos = xsp;
    return false;
  }

 inline bool jj_3R_host_parameter_name_1007_6_727()
 {
    if (jj_done) return true;
    if (jj_scan_token(568)) return true;
    if (jj_3R_identifier_928_3_141()) return true;
    return false;
  }

 inline bool jj_3_1953()
 {
    if (jj_done) return true;
    if (jj_scan_token(570)) return true;
    if (jj_3R_schema_qualified_name_970_5_252()) return true;
    return false;
  }

 inline bool jj_3R_commit_statement_7147_5_779()
 {
    if (jj_done) return true;
    if (jj_scan_token(COMMIT)) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1956()) jj_scanpos = xsp;
    xsp = jj_scanpos;
    if (jj_3_1958()) jj_scanpos = xsp;
    return false;
  }

 inline bool jj_3R_release_savepoint_statement_7141_5_778()
 {
    if (jj_done) return true;
    if (jj_scan_token(RELEASE)) return true;
    if (jj_scan_token(SAVEPOINT)) return true;
    if (jj_3R_savepoint_specifier_7135_5_996()) return true;
    return false;
  }

 inline bool jj_3R_cursor_name_995_5_144()
 {
    if (jj_done) return true;
    if (jj_3R_identifier_chain_1496_5_207()) return true;
    return false;
  }

 inline bool jj_3R_savepoint_specifier_7135_5_996()
 {
    if (jj_done) return true;
    if (jj_3R_identifier_928_3_141()) return true;
    return false;
  }

 inline bool jj_3_391()
 {
    if (jj_done) return true;
    if (jj_3R_schema_name_956_5_140()) return true;
    return false;
  }

 inline bool jj_3R_savepoint_statement_7129_5_777()
 {
    if (jj_done) return true;
    if (jj_scan_token(SAVEPOINT)) return true;
    if (jj_3R_savepoint_specifier_7135_5_996()) return true;
    return false;
  }

 inline bool jj_3_390()
 {
    if (jj_done) return true;
    if (jj_scan_token(280)) return true;
    return false;
  }

 inline bool jj_3_1955()
 {
    if (jj_done) return true;
    if (jj_3R_schema_qualified_name_970_5_252()) return true;
    Token * xsp;
    while (true) {
      xsp = jj_scanpos;
      if (jj_3_1953()) { jj_scanpos = xsp; break; }
    }
    return false;
  }

 inline bool jj_3R_constraint_name_list_7122_5_995()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1954()) {
    jj_scanpos = xsp;
    if (jj_3_1955()) return true;
    }
    return false;
  }

 inline bool jj_3_1954()
 {
    if (jj_done) return true;
    if (jj_scan_token(ALL)) return true;
    return false;
  }

 inline bool jj_3R_set_constraints_mode_statement_7116_5_776()
 {
    if (jj_done) return true;
    if (jj_scan_token(SET)) return true;
    if (jj_scan_token(CONSTRAINTS)) return true;
    if (jj_3R_constraint_name_list_7122_5_995()) return true;
    return false;
  }

 inline bool jj_3R_schema_qualified_name_970_5_252()
 {
    if (jj_done) return true;
    if (jj_3R_identifier_chain_1496_5_207()) return true;
    return false;
  }

 inline bool jj_3R_diagnostics_size_7110_5_840()
 {
    if (jj_done) return true;
    if (jj_scan_token(DIAGNOSTICS)) return true;
    if (jj_scan_token(SIZE)) return true;
    if (jj_3R_simple_value_specification_1423_5_220()) return true;
    return false;
  }

 inline bool jj_3_1950()
 {
    if (jj_done) return true;
    if (jj_scan_token(SERIALIZABLE)) return true;
    return false;
  }

 inline bool jj_3_1949()
 {
    if (jj_done) return true;
    if (jj_scan_token(REPEATABLE)) return true;
    if (jj_scan_token(READ)) return true;
    return false;
  }

 inline bool jj_3_1940()
 {
    if (jj_done) return true;
    if (jj_3R_transaction_characteristics_7074_5_836()) return true;
    return false;
  }

 inline bool jj_3_1948()
 {
    if (jj_done) return true;
    if (jj_scan_token(READ)) return true;
    if (jj_scan_token(COMMITTED)) return true;
    return false;
  }

 inline bool jj_3R_level_of_isolation_7101_5_1012()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1947()) {
    jj_scanpos = xsp;
    if (jj_3_1948()) {
    jj_scanpos = xsp;
    if (jj_3_1949()) {
    jj_scanpos = xsp;
    if (jj_3_1950()) return true;
    }
    }
    }
    return false;
  }

 inline bool jj_3_1947()
 {
    if (jj_done) return true;
    if (jj_scan_token(READ)) return true;
    if (jj_scan_token(UNCOMMITTED)) return true;
    return false;
  }

 inline bool jj_3R_schema_name_956_5_140()
 {
    if (jj_done) return true;
    if (jj_3R_identifier_chain_1496_5_207()) return true;
    return false;
  }

 inline bool jj_3R_isolation_level_7095_5_838()
 {
    if (jj_done) return true;
    if (jj_scan_token(ISOLATION)) return true;
    if (jj_scan_token(LEVEL)) return true;
    if (jj_3R_level_of_isolation_7101_5_1012()) return true;
    return false;
  }

 inline bool jj_3_1941()
 {
    if (jj_done) return true;
    if (jj_scan_token(570)) return true;
    if (jj_3R_transaction_mode_7080_5_837()) return true;
    return false;
  }

 inline bool jj_3R_table_name_948_5_382()
 {
    if (jj_done) return true;
    if (jj_3R_identifier_chain_1496_5_207()) return true;
    return false;
  }

 inline bool jj_3_1946()
 {
    if (jj_done) return true;
    if (jj_scan_token(READ)) return true;
    if (jj_scan_token(WRITE)) return true;
    return false;
  }

 inline bool jj_3R_transaction_access_mode_7088_5_839()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1945()) {
    jj_scanpos = xsp;
    if (jj_3_1946()) return true;
    }
    return false;
  }

 inline bool jj_3_1945()
 {
    if (jj_done) return true;
    if (jj_scan_token(READ)) return true;
    if (jj_scan_token(ONLY)) return true;
    return false;
  }

 inline bool jj_3_1938()
 {
    if (jj_done) return true;
    if (jj_3R_transaction_characteristics_7074_5_836()) return true;
    return false;
  }

 inline bool jj_3_389()
 {
    if (jj_done) return true;
    if (jj_3R_non_reserved_word_210_5_139()) return true;
    return false;
  }

 inline bool jj_3_388()
 {
    if (jj_done) return true;
    if (jj_scan_token(Unicode_delimited_identifier)) return true;
    return false;
  }

 inline bool jj_3_387()
 {
    if (jj_done) return true;
    if (jj_scan_token(delimited_identifier)) return true;
    return false;
  }

 inline bool jj_3_386()
 {
    if (jj_done) return true;
    if (jj_scan_token(regular_identifier)) return true;
    return false;
  }

 inline bool jj_3R_actual_identifier_939_5_137()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_386()) {
    jj_scanpos = xsp;
    if (jj_3_387()) {
    jj_scanpos = xsp;
    if (jj_3_388()) {
    jj_scanpos = xsp;
    jj_lookingAhead = true;
    jj_semLA = IsIdNonReservedWord();
    jj_lookingAhead = false;
    if (!jj_semLA || jj_3_389()) return true;
    }
    }
    }
    return false;
  }

 inline bool jj_3_1944()
 {
    if (jj_done) return true;
    if (jj_3R_diagnostics_size_7110_5_840()) return true;
    return false;
  }

 inline bool jj_3_1943()
 {
    if (jj_done) return true;
    if (jj_3R_transaction_access_mode_7088_5_839()) return true;
    return false;
  }

 inline bool jj_3R_transaction_mode_7080_5_837()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1942()) {
    jj_scanpos = xsp;
    if (jj_3_1943()) {
    jj_scanpos = xsp;
    if (jj_3_1944()) return true;
    }
    }
    return false;
  }

 inline bool jj_3_1942()
 {
    if (jj_done) return true;
    if (jj_3R_isolation_level_7095_5_838()) return true;
    return false;
  }

 inline bool jj_3_385()
 {
    if (jj_done) return true;
    if (jj_3R_identifier_suffix_chain_7944_5_138()) return true;
    return false;
  }

 inline bool jj_3_1939()
 {
    if (jj_done) return true;
    if (jj_scan_token(LOCAL)) return true;
    return false;
  }

 inline bool jj_3R_transaction_characteristics_7074_5_836()
 {
    if (jj_done) return true;
    if (jj_3R_transaction_mode_7080_5_837()) return true;
    Token * xsp;
    while (true) {
      xsp = jj_scanpos;
      if (jj_3_1941()) { jj_scanpos = xsp; break; }
    }
    return false;
  }

 inline bool jj_3_384()
 {
    if (jj_done) return true;
    if (jj_scan_token(561)) return true;
    return false;
  }

 inline bool jj_3_377()
 {
    if (jj_done) return true;
    if (jj_scan_token(MINUS)) return true;
    return false;
  }

 inline bool jj_3_383()
 {
    if (jj_done) return true;
    if (jj_3R_actual_identifier_939_5_137()) return true;
    return false;
  }

 inline bool jj_3R_identifier_928_3_141()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_383()) {
    jj_scanpos = xsp;
    if (jj_3_384()) return true;
    }
    xsp = jj_scanpos;
    if (jj_3_385()) jj_scanpos = xsp;
    return false;
  }

 inline bool jj_3R_set_transaction_statement_7068_5_775()
 {
    if (jj_done) return true;
    if (jj_scan_token(SET)) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1939()) jj_scanpos = xsp;
    if (jj_scan_token(TRANSACTION)) return true;
    xsp = jj_scanpos;
    if (jj_3_1940()) jj_scanpos = xsp;
    return false;
  }

 inline bool jj_3_376()
 {
    if (jj_done) return true;
    if (jj_scan_token(PLUS)) return true;
    return false;
  }

 inline bool jj_3_378()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_376()) {
    jj_scanpos = xsp;
    if (jj_3_377()) return true;
    }
    return false;
  }

 inline bool jj_3_382()
 {
    if (jj_done) return true;
    if (jj_scan_token(UNKNOWN)) return true;
    return false;
  }

 inline bool jj_3_380()
 {
    if (jj_done) return true;
    if (jj_scan_token(FALSE)) return true;
    return false;
  }

 inline bool jj_3R_start_transaction_statement_7062_5_774()
 {
    if (jj_done) return true;
    if (jj_scan_token(START)) return true;
    if (jj_scan_token(TRANSACTION)) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1938()) jj_scanpos = xsp;
    return false;
  }

 inline bool jj_3_379()
 {
    if (jj_done) return true;
    if (jj_scan_token(TRUE)) return true;
    return false;
  }

 inline bool jj_3_381()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_379()) {
    jj_scanpos = xsp;
    if (jj_3_380()) return true;
    }
    return false;
  }

 inline bool jj_3R_boolean_literal_918_3_132()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_381()) {
    jj_scanpos = xsp;
    if (jj_3_382()) return true;
    }
    return false;
  }

 inline bool jj_3_1937()
 {
    if (jj_done) return true;
    if (jj_scan_token(NULL_)) return true;
    return false;
  }

 inline bool jj_3R_return_value_7055_5_994()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1936()) {
    jj_scanpos = xsp;
    if (jj_3_1937()) return true;
    }
    return false;
  }

 inline bool jj_3_1936()
 {
    if (jj_done) return true;
    if (jj_3R_value_expression_1855_5_178()) return true;
    return false;
  }

 inline bool jj_3_371()
 {
    if (jj_done) return true;
    if (jj_scan_token(quoted_string)) return true;
    return false;
  }

 inline bool jj_3R_interval_literal_910_5_131()
 {
    if (jj_done) return true;
    if (jj_scan_token(INTERVAL)) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_378()) jj_scanpos = xsp;
    if (jj_3R_character_string_literal_866_3_128()) return true;
    if (jj_3R_interval_qualifier_3874_5_331()) return true;
    return false;
  }

 inline bool jj_3R_return_statement_7049_5_773()
 {
    if (jj_done) return true;
    if (jj_scan_token(RETURN)) return true;
    if (jj_3R_return_value_7055_5_994()) return true;
    return false;
  }

 inline bool jj_3R_timestamp_literal_904_5_136()
 {
    if (jj_done) return true;
    if (jj_scan_token(TIMESTAMP)) return true;
    if (jj_3R_character_string_literal_866_3_128()) return true;
    return false;
  }

 inline bool jj_3R_call_statement_7043_5_772()
 {
    if (jj_done) return true;
    if (jj_scan_token(CALL)) return true;
    if (jj_3R_routine_invocation_3966_5_257()) return true;
    return false;
  }

 inline bool jj_3R_time_literal_898_5_135()
 {
    if (jj_done) return true;
    if (jj_scan_token(TIME)) return true;
    if (jj_3R_character_string_literal_866_3_128()) return true;
    return false;
  }

 inline bool jj_3_1935()
 {
    if (jj_done) return true;
    if (jj_scan_token(ON)) return true;
    if (jj_scan_token(COMMIT)) return true;
    if (jj_3R_table_commit_action_4450_5_525()) return true;
    return false;
  }

 inline bool jj_3R_temporary_table_declaration_7036_5_718()
 {
    if (jj_done) return true;
    if (jj_scan_token(DECLARE)) return true;
    if (jj_scan_token(LOCAL)) return true;
    if (jj_scan_token(TEMPORARY)) return true;
    return false;
  }

 inline bool jj_3R_date_literal_892_5_134()
 {
    if (jj_done) return true;
    if (jj_scan_token(DATE)) return true;
    if (jj_3R_character_string_literal_866_3_128()) return true;
    return false;
  }

 inline bool jj_3_1930()
 {
    if (jj_done) return true;
    if (jj_scan_token(569)) return true;
    if (jj_3R_identifier_928_3_141()) return true;
    return false;
  }

 inline bool jj_3_1934()
 {
    if (jj_done) return true;
    if (jj_3R_contextually_typed_value_specification_1475_5_194()) return true;
    return false;
  }

 inline bool jj_3R_update_source_7029_5_833()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1933()) {
    jj_scanpos = xsp;
    if (jj_3_1934()) return true;
    }
    return false;
  }

 inline bool jj_3_375()
 {
    if (jj_done) return true;
    if (jj_3R_timestamp_literal_904_5_136()) return true;
    return false;
  }

 inline bool jj_3_1933()
 {
    if (jj_done) return true;
    if (jj_3R_value_expression_1855_5_178()) return true;
    return false;
  }

 inline bool jj_3_374()
 {
    if (jj_done) return true;
    if (jj_3R_time_literal_898_5_135()) return true;
    return false;
  }

 inline bool jj_3_373()
 {
    if (jj_done) return true;
    if (jj_3R_date_literal_892_5_134()) return true;
    return false;
  }

 inline bool jj_3R_datetime_literal_884_5_130()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_373()) {
    jj_scanpos = xsp;
    if (jj_3_374()) {
    jj_scanpos = xsp;
    if (jj_3_375()) return true;
    }
    }
    return false;
  }

 inline bool jj_3_1929()
 {
    if (jj_done) return true;
    if (jj_3R_identifier_928_3_141()) return true;
    if (jj_3R_left_bracket_or_trigraph_804_5_174()) return true;
    if (jj_3R_simple_value_specification_1423_5_220()) return true;
    if (jj_3R_right_bracket_or_trigraph_811_5_175()) return true;
    return false;
  }

 inline bool jj_3_372()
 {
    if (jj_done) return true;
    if (jj_scan_token(underscore)) return true;
    if (jj_3R_character_set_specification_4015_5_133()) return true;
    return false;
  }

 inline bool jj_3_1932()
 {
    if (jj_done) return true;
    if (jj_3R_mutated_set_clause_7016_5_835()) return true;
    return false;
  }

 inline bool jj_3_1931()
 {
    if (jj_done) return true;
    if (jj_3R_identifier_928_3_141()) return true;
    return false;
  }

 inline bool jj_3R_Unicode_character_string_literal_878_5_129()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_372()) jj_scanpos = xsp;
    if (jj_scan_token(unicode_literal)) return true;
    return false;
  }

 inline bool jj_3_1927()
 {
    if (jj_done) return true;
    if (jj_scan_token(570)) return true;
    if (jj_3R_set_target_6984_5_832()) return true;
    return false;
  }

 inline bool jj_3R_mutated_set_clause_7016_5_835()
 {
    if (jj_done) return true;
    if (jj_3R_identifier_928_3_141()) return true;
    Token * xsp;
    if (jj_3_1930()) return true;
    while (true) {
      xsp = jj_scanpos;
      if (jj_3_1930()) { jj_scanpos = xsp; break; }
    }
    return false;
  }

 inline bool jj_3_370()
 {
    if (jj_done) return true;
    if (jj_scan_token(underscore)) return true;
    if (jj_3R_character_set_specification_4015_5_133()) return true;
    return false;
  }

 inline bool jj_3R_update_target_7009_5_834()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1928()) {
    jj_scanpos = xsp;
    if (jj_3_1929()) return true;
    }
    return false;
  }

 inline bool jj_3_1928()
 {
    if (jj_done) return true;
    if (jj_3R_identifier_928_3_141()) return true;
    return false;
  }

 inline bool jj_3R_character_string_literal_866_3_128()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_370()) jj_scanpos = xsp;
    if (jj_3_371()) return true;
    while (true) {
      xsp = jj_scanpos;
      if (jj_3_371()) { jj_scanpos = xsp; break; }
    }
    return false;
  }

 inline bool jj_3R_assigned_row_7003_5_1056()
 {
    if (jj_done) return true;
    if (jj_3R_contextually_typed_row_value_expression_2751_5_364()) return true;
    return false;
  }

 inline bool jj_3_369()
 {
    if (jj_done) return true;
    if (jj_3R_boolean_literal_918_3_132()) return true;
    return false;
  }

 inline bool jj_3_368()
 {
    if (jj_done) return true;
    if (jj_3R_interval_literal_910_5_131()) return true;
    return false;
  }

 inline bool jj_3_367()
 {
    if (jj_done) return true;
    if (jj_3R_datetime_literal_884_5_130()) return true;
    return false;
  }

 inline bool jj_3_366()
 {
    if (jj_done) return true;
    if (jj_scan_token(binary_string_literal)) return true;
    return false;
  }

 inline bool jj_3R_set_target_list_6997_6_1011()
 {
    if (jj_done) return true;
    if (jj_scan_token(lparen)) return true;
    if (jj_3R_set_target_6984_5_832()) return true;
    Token * xsp;
    while (true) {
      xsp = jj_scanpos;
      if (jj_3_1927()) { jj_scanpos = xsp; break; }
    }
    if (jj_scan_token(rparen)) return true;
    return false;
  }

 inline bool jj_3_365()
 {
    if (jj_done) return true;
    if (jj_3R_Unicode_character_string_literal_878_5_129()) return true;
    return false;
  }

 inline bool jj_3_364()
 {
    if (jj_done) return true;
    if (jj_scan_token(national_character_string_literal)) return true;
    return false;
  }

 inline bool jj_3_363()
 {
    if (jj_done) return true;
    if (jj_3R_character_string_literal_866_3_128()) return true;
    return false;
  }

 inline bool jj_3R_general_literal_853_5_125()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_363()) {
    jj_scanpos = xsp;
    if (jj_3_364()) {
    jj_scanpos = xsp;
    if (jj_3_365()) {
    jj_scanpos = xsp;
    if (jj_3_366()) {
    jj_scanpos = xsp;
    if (jj_3_367()) {
    jj_scanpos = xsp;
    if (jj_3_368()) {
    jj_scanpos = xsp;
    if (jj_3_369()) return true;
    }
    }
    }
    }
    }
    }
    return false;
  }

 inline bool jj_3R_multiple_column_assignment_6991_5_831()
 {
    if (jj_done) return true;
    if (jj_3R_set_target_list_6997_6_1011()) return true;
    if (jj_scan_token(EQUAL)) return true;
    if (jj_3R_assigned_row_7003_5_1056()) return true;
    return false;
  }

 inline bool jj_3_1919()
 {
    if (jj_done) return true;
    if (jj_scan_token(AS)) return true;
    return false;
  }

 inline bool jj_3_362()
 {
    if (jj_done) return true;
    if (jj_scan_token(float_literal)) return true;
    return false;
  }

 inline bool jj_3_361()
 {
    if (jj_done) return true;
    if (jj_scan_token(unsigned_integer)) return true;
    return false;
  }

 inline bool jj_3R_exact_numeric_literal_846_5_127()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_361()) {
    jj_scanpos = xsp;
    if (jj_3_362()) return true;
    }
    return false;
  }

 inline bool jj_3_1920()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1919()) jj_scanpos = xsp;
    if (jj_3R_identifier_928_3_141()) return true;
    return false;
  }

 inline bool jj_3_1922()
 {
    if (jj_done) return true;
    if (jj_scan_token(570)) return true;
    if (jj_3R_set_clause_6977_5_830()) return true;
    return false;
  }

 inline bool jj_3_1926()
 {
    if (jj_done) return true;
    if (jj_3R_mutated_set_clause_7016_5_835()) return true;
    return false;
  }

 inline bool jj_3R_set_target_6984_5_832()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1925()) {
    jj_scanpos = xsp;
    if (jj_3_1926()) return true;
    }
    return false;
  }

 inline bool jj_3_1925()
 {
    if (jj_done) return true;
    if (jj_3R_update_target_7009_5_834()) return true;
    return false;
  }

 inline bool jj_3_360()
 {
    if (jj_done) return true;
    if (jj_scan_token(approximate_numeric_literal)) return true;
    return false;
  }

 inline bool jj_3_1917()
 {
    if (jj_done) return true;
    if (jj_scan_token(AS)) return true;
    return false;
  }

 inline bool jj_3_359()
 {
    if (jj_done) return true;
    if (jj_3R_exact_numeric_literal_846_5_127()) return true;
    return false;
  }

 inline bool jj_3R_unsigned_numeric_literal_839_5_126()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_359()) {
    jj_scanpos = xsp;
    if (jj_3_360()) return true;
    }
    return false;
  }

 inline bool jj_3_1918()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1917()) jj_scanpos = xsp;
    if (jj_3R_identifier_928_3_141()) return true;
    return false;
  }

 inline bool jj_3_354()
 {
    if (jj_done) return true;
    if (jj_scan_token(MINUS)) return true;
    return false;
  }

 inline bool jj_3_1924()
 {
    if (jj_done) return true;
    if (jj_3R_set_target_6984_5_832()) return true;
    if (jj_scan_token(EQUAL)) return true;
    if (jj_3R_update_source_7029_5_833()) return true;
    return false;
  }

 inline bool jj_3R_set_clause_6977_5_830()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1923()) {
    jj_scanpos = xsp;
    if (jj_3_1924()) return true;
    }
    return false;
  }

 inline bool jj_3_1923()
 {
    if (jj_done) return true;
    if (jj_3R_multiple_column_assignment_6991_5_831()) return true;
    return false;
  }

 inline bool jj_3_358()
 {
    if (jj_done) return true;
    if (jj_3R_general_literal_853_5_125()) return true;
    return false;
  }

 inline bool jj_3_357()
 {
    if (jj_done) return true;
    if (jj_3R_unsigned_numeric_literal_839_5_126()) return true;
    return false;
  }

 inline bool jj_3R_unsigned_literal_832_5_205()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_357()) {
    jj_scanpos = xsp;
    if (jj_3_358()) return true;
    }
    return false;
  }

 inline bool jj_3_353()
 {
    if (jj_done) return true;
    if (jj_scan_token(PLUS)) return true;
    return false;
  }

 inline bool jj_3_1914()
 {
    if (jj_done) return true;
    if (jj_scan_token(570)) return true;
    if (jj_3R_merge_insert_value_element_6948_5_829()) return true;
    return false;
  }

 inline bool jj_3R_set_clause_list_6971_5_1010()
 {
    if (jj_done) return true;
    if (jj_3R_set_clause_6977_5_830()) return true;
    Token * xsp;
    while (true) {
      xsp = jj_scanpos;
      if (jj_3_1922()) { jj_scanpos = xsp; break; }
    }
    return false;
  }

 inline bool jj_3_356()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_353()) {
    jj_scanpos = xsp;
    if (jj_3_354()) return true;
    }
    if (jj_3R_unsigned_numeric_literal_839_5_126()) return true;
    return false;
  }

 inline bool jj_3_355()
 {
    if (jj_done) return true;
    if (jj_3R_unsigned_numeric_literal_839_5_126()) return true;
    return false;
  }

 inline bool jj_3R_signed_numeric_literal_825_5_124()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_355()) {
    jj_scanpos = xsp;
    if (jj_3_356()) return true;
    }
    return false;
  }

 inline bool jj_3_1921()
 {
    if (jj_done) return true;
    if (jj_scan_token(WHERE)) return true;
    if (jj_3R_search_condition_3868_5_818()) return true;
    return false;
  }

 inline bool jj_3R_update_statement_searched_6963_5_386()
 {
    if (jj_done) return true;
    if (jj_scan_token(UPDATE)) return true;
    if (jj_3R_target_table_6796_5_877()) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1920()) jj_scanpos = xsp;
    if (jj_scan_token(SET)) return true;
    if (jj_3R_set_clause_list_6971_5_1010()) return true;
    xsp = jj_scanpos;
    if (jj_3_1921()) jj_scanpos = xsp;
    return false;
  }

 inline bool jj_3_352()
 {
    if (jj_done) return true;
    if (jj_3R_general_literal_853_5_125()) return true;
    return false;
  }

 inline bool jj_3_351()
 {
    if (jj_done) return true;
    if (jj_3R_signed_numeric_literal_825_5_124()) return true;
    return false;
  }

 inline bool jj_3R_literal_818_5_203()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_351()) {
    jj_scanpos = xsp;
    if (jj_3_352()) return true;
    }
    return false;
  }

 inline bool jj_3R_update_statement_positioned_6955_5_770()
 {
    if (jj_done) return true;
    if (jj_scan_token(UPDATE)) return true;
    if (jj_3R_target_table_6796_5_877()) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1918()) jj_scanpos = xsp;
    if (jj_scan_token(SET)) return true;
    return false;
  }

 inline bool jj_3_350()
 {
    if (jj_done) return true;
    if (jj_scan_token(565)) return true;
    return false;
  }

 inline bool jj_3_349()
 {
    if (jj_done) return true;
    if (jj_scan_token(564)) return true;
    return false;
  }

 inline bool jj_3R_right_bracket_or_trigraph_811_5_175()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_349()) {
    jj_scanpos = xsp;
    if (jj_3_350()) return true;
    }
    return false;
  }

 inline bool jj_3_1916()
 {
    if (jj_done) return true;
    if (jj_3R_contextually_typed_value_specification_1475_5_194()) return true;
    return false;
  }

 inline bool jj_3R_merge_insert_value_element_6948_5_829()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1915()) {
    jj_scanpos = xsp;
    if (jj_3_1916()) return true;
    }
    return false;
  }

 inline bool jj_3_348()
 {
    if (jj_done) return true;
    if (jj_scan_token(563)) return true;
    return false;
  }

 inline bool jj_3_1915()
 {
    if (jj_done) return true;
    if (jj_3R_value_expression_1855_5_178()) return true;
    return false;
  }

 inline bool jj_3_347()
 {
    if (jj_done) return true;
    if (jj_scan_token(562)) return true;
    return false;
  }

 inline bool jj_3R_left_bracket_or_trigraph_804_5_174()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_347()) {
    jj_scanpos = xsp;
    if (jj_3_348()) return true;
    }
    return false;
  }

 inline bool jj_3_1912()
 {
    if (jj_done) return true;
    if (jj_scan_token(lparen)) return true;
    if (jj_3R_insert_column_list_6872_5_823()) return true;
    if (jj_scan_token(rparen)) return true;
    return false;
  }

 inline bool jj_3R_merge_insert_value_list_6940_6_1059()
 {
    if (jj_done) return true;
    if (jj_scan_token(lparen)) return true;
    if (jj_3R_merge_insert_value_element_6948_5_829()) return true;
    Token * xsp;
    while (true) {
      xsp = jj_scanpos;
      if (jj_3_1914()) { jj_scanpos = xsp; break; }
    }
    if (jj_scan_token(rparen)) return true;
    return false;
  }

 inline bool jj_3_1911()
 {
    if (jj_done) return true;
    if (jj_scan_token(AND)) return true;
    if (jj_3R_search_condition_3868_5_818()) return true;
    return false;
  }

 inline bool jj_3_1913()
 {
    if (jj_done) return true;
    if (jj_3R_override_clause_6859_5_824()) return true;
    return false;
  }

 inline bool jj_3R_merge_insert_specification_6932_5_1058()
 {
    if (jj_done) return true;
    if (jj_scan_token(INSERT)) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1912()) jj_scanpos = xsp;
    xsp = jj_scanpos;
    if (jj_3_1913()) jj_scanpos = xsp;
    if (jj_scan_token(VALUES)) return true;
    if (jj_3R_merge_insert_value_list_6940_6_1059()) return true;
    return false;
  }

 inline bool jj_3R_merge_update_specification_6920_5_828()
 {
    if (jj_done) return true;
    if (jj_scan_token(UPDATE)) return true;
    if (jj_scan_token(SET)) return true;
    if (jj_3R_set_clause_list_6971_5_1010()) return true;
    return false;
  }

 inline bool jj_3_1908()
 {
    if (jj_done) return true;
    if (jj_scan_token(AND)) return true;
    if (jj_3R_search_condition_3868_5_818()) return true;
    return false;
  }

 inline bool jj_3R_merge_when_not_matched_clause_6913_5_827()
 {
    if (jj_done) return true;
    if (jj_scan_token(WHEN)) return true;
    if (jj_scan_token(NOT)) return true;
    if (jj_scan_token(MATCHED)) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1911()) jj_scanpos = xsp;
    if (jj_scan_token(THEN)) return true;
    if (jj_3R_merge_insert_specification_6932_5_1058()) return true;
    return false;
  }

 inline bool jj_3_1903()
 {
    if (jj_done) return true;
    if (jj_scan_token(AS)) return true;
    return false;
  }

 inline bool jj_3_1904()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1903()) jj_scanpos = xsp;
    if (jj_3R_identifier_928_3_141()) return true;
    return false;
  }

 inline bool jj_3_1910()
 {
    if (jj_done) return true;
    if (jj_scan_token(395)) return true;
    return false;
  }

 inline bool jj_3R_merge_update_or_delete_specification_6906_5_1057()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1909()) {
    jj_scanpos = xsp;
    if (jj_3_1910()) return true;
    }
    return false;
  }

 inline bool jj_3_1909()
 {
    if (jj_done) return true;
    if (jj_3R_merge_update_specification_6920_5_828()) return true;
    return false;
  }

 inline bool jj_3R_merge_when_matched_clause_6899_5_826()
 {
    if (jj_done) return true;
    if (jj_scan_token(WHEN)) return true;
    if (jj_scan_token(MATCHED)) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1908()) jj_scanpos = xsp;
    if (jj_scan_token(THEN)) return true;
    if (jj_3R_merge_update_or_delete_specification_6906_5_1057()) return true;
    return false;
  }

 inline bool jj_3_1907()
 {
    if (jj_done) return true;
    if (jj_3R_merge_when_not_matched_clause_6913_5_827()) return true;
    return false;
  }

 inline bool jj_3R_merge_when_clause_6892_5_825()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1906()) {
    jj_scanpos = xsp;
    if (jj_3_1907()) return true;
    }
    return false;
  }

 inline bool jj_3_1906()
 {
    if (jj_done) return true;
    if (jj_3R_merge_when_matched_clause_6899_5_826()) return true;
    return false;
  }

 inline bool jj_3_1905()
 {
    if (jj_done) return true;
    if (jj_3R_merge_when_clause_6892_5_825()) return true;
    return false;
  }

 inline bool jj_3R_merge_operation_specification_6886_5_1055()
 {
    if (jj_done) return true;
    Token * xsp;
    if (jj_3_1905()) return true;
    while (true) {
      xsp = jj_scanpos;
      if (jj_3_1905()) { jj_scanpos = xsp; break; }
    }
    return false;
  }

 inline bool jj_3R_merge_statement_6878_5_385()
 {
    if (jj_done) return true;
    if (jj_scan_token(MERGE)) return true;
    if (jj_scan_token(INTO)) return true;
    if (jj_3R_target_table_6796_5_877()) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1904()) jj_scanpos = xsp;
    if (jj_scan_token(USING)) return true;
    if (jj_3R_table_reference_2819_5_369()) return true;
    if (jj_scan_token(ON)) return true;
    if (jj_3R_search_condition_3868_5_818()) return true;
    if (jj_3R_merge_operation_specification_6886_5_1055()) return true;
    return false;
  }

 inline bool jj_3R_insert_column_list_6872_5_823()
 {
    if (jj_done) return true;
    if (jj_3R_column_name_list_2942_5_191()) return true;
    return false;
  }

 inline bool jj_3R_from_default_6866_5_822()
 {
    if (jj_done) return true;
    if (jj_scan_token(DEFAULT_)) return true;
    if (jj_scan_token(VALUES)) return true;
    return false;
  }

 inline bool jj_3_1902()
 {
    if (jj_done) return true;
    if (jj_scan_token(OVERRIDING)) return true;
    if (jj_scan_token(SYSTEM)) return true;
    if (jj_scan_token(VALUE)) return true;
    return false;
  }

 inline bool jj_3R_override_clause_6859_5_824()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1901()) {
    jj_scanpos = xsp;
    if (jj_3_1902()) return true;
    }
    return false;
  }

 inline bool jj_3_1901()
 {
    if (jj_done) return true;
    if (jj_scan_token(OVERRIDING)) return true;
    if (jj_scan_token(USER)) return true;
    if (jj_scan_token(VALUE)) return true;
    return false;
  }

 inline bool jj_3_1900()
 {
    if (jj_done) return true;
    if (jj_3R_override_clause_6859_5_824()) return true;
    return false;
  }

 inline bool jj_3_1899()
 {
    if (jj_done) return true;
    if (jj_scan_token(lparen)) return true;
    if (jj_3R_insert_column_list_6872_5_823()) return true;
    if (jj_scan_token(rparen)) return true;
    return false;
  }

 inline bool jj_3R_from_constructor_6851_5_821()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1899()) jj_scanpos = xsp;
    xsp = jj_scanpos;
    if (jj_3_1900()) jj_scanpos = xsp;
    if (jj_3R_contextually_typed_table_value_constructor_2784_5_1009()) return true;
    return false;
  }

 inline bool jj_3_1891()
 {
    if (jj_done) return true;
    if (jj_3R_identity_column_restart_option_6816_5_819()) return true;
    return false;
  }

 inline bool jj_3_1898()
 {
    if (jj_done) return true;
    if (jj_3R_override_clause_6859_5_824()) return true;
    return false;
  }

 inline bool jj_3_1897()
 {
    if (jj_done) return true;
    if (jj_scan_token(lparen)) return true;
    if (jj_3R_insert_column_list_6872_5_823()) return true;
    if (jj_scan_token(rparen)) return true;
    return false;
  }

 inline bool jj_3R_from_subquery_6843_5_820()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1897()) jj_scanpos = xsp;
    xsp = jj_scanpos;
    if (jj_3_1898()) jj_scanpos = xsp;
    if (jj_3R_query_expression_3399_5_547()) return true;
    return false;
  }

 inline bool jj_3_1888()
 {
    if (jj_done) return true;
    if (jj_scan_token(AS)) return true;
    return false;
  }

 inline bool jj_3_1896()
 {
    if (jj_done) return true;
    if (jj_3R_from_default_6866_5_822()) return true;
    return false;
  }

 inline bool jj_3_1889()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1888()) jj_scanpos = xsp;
    if (jj_3R_identifier_928_3_141()) return true;
    return false;
  }

 inline bool jj_3_1895()
 {
    if (jj_done) return true;
    if (jj_3R_from_constructor_6851_5_821()) return true;
    return false;
  }

 inline bool jj_3R_insert_columns_and_source_6835_5_1054()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1894()) {
    jj_scanpos = xsp;
    if (jj_3_1895()) {
    jj_scanpos = xsp;
    if (jj_3_1896()) return true;
    }
    }
    return false;
  }

 inline bool jj_3_1894()
 {
    if (jj_done) return true;
    if (jj_3R_from_subquery_6843_5_820()) return true;
    return false;
  }

 inline bool jj_3R_insertion_target_6829_5_927()
 {
    if (jj_done) return true;
    if (jj_3R_table_name_948_5_382()) return true;
    return false;
  }

 inline bool jj_3_1884()
 {
    if (jj_done) return true;
    if (jj_scan_token(AS)) return true;
    return false;
  }

 inline bool jj_3R_insert_statement_6823_5_384()
 {
    if (jj_done) return true;
    if (jj_scan_token(INSERT)) return true;
    if (jj_scan_token(INTO)) return true;
    if (jj_3R_insertion_target_6829_5_927()) return true;
    if (jj_3R_insert_columns_and_source_6835_5_1054()) return true;
    return false;
  }

 inline bool jj_3_1885()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1884()) jj_scanpos = xsp;
    if (jj_3R_identifier_928_3_141()) return true;
    return false;
  }

 inline bool jj_3_1893()
 {
    if (jj_done) return true;
    if (jj_scan_token(RESTART)) return true;
    if (jj_scan_token(IDENTITY)) return true;
    return false;
  }

 inline bool jj_3R_identity_column_restart_option_6816_5_819()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1892()) {
    jj_scanpos = xsp;
    if (jj_3_1893()) return true;
    }
    return false;
  }

 inline bool jj_3_1892()
 {
    if (jj_done) return true;
    if (jj_scan_token(CONTINUE)) return true;
    if (jj_scan_token(IDENTITY)) return true;
    return false;
  }

 inline bool jj_3R_truncate_table_statement_6810_5_771()
 {
    if (jj_done) return true;
    if (jj_scan_token(TRUNCATE)) return true;
    if (jj_scan_token(TABLE)) return true;
    if (jj_3R_target_table_6796_5_877()) return true;
    return false;
  }

 inline bool jj_3_1873()
 {
    if (jj_done) return true;
    if (jj_scan_token(RELATIVE)) return true;
    return false;
  }

 inline bool jj_3_1883()
 {
    if (jj_done) return true;
    if (jj_scan_token(570)) return true;
    if (jj_3R_target_specification_1434_3_475()) return true;
    return false;
  }

 inline bool jj_3_1890()
 {
    if (jj_done) return true;
    if (jj_scan_token(WHERE)) return true;
    if (jj_3R_search_condition_3868_5_818()) return true;
    return false;
  }

 inline bool jj_3R_delete_statement_searched_6803_5_383()
 {
    if (jj_done) return true;
    if (jj_scan_token(DELETE)) return true;
    if (jj_scan_token(FROM)) return true;
    if (jj_3R_target_table_6796_5_877()) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1889()) jj_scanpos = xsp;
    xsp = jj_scanpos;
    if (jj_3_1890()) jj_scanpos = xsp;
    return false;
  }

 inline bool jj_3_1872()
 {
    if (jj_done) return true;
    if (jj_scan_token(ABSOLUTE)) return true;
    return false;
  }

 inline bool jj_3_1887()
 {
    if (jj_done) return true;
    if (jj_scan_token(ONLY)) return true;
    if (jj_scan_token(lparen)) return true;
    if (jj_3R_table_name_948_5_382()) return true;
    if (jj_scan_token(rparen)) return true;
    return false;
  }

 inline bool jj_3R_target_table_6796_5_877()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1886()) {
    jj_scanpos = xsp;
    if (jj_3_1887()) return true;
    }
    return false;
  }

 inline bool jj_3_1886()
 {
    if (jj_done) return true;
    if (jj_3R_table_name_948_5_382()) return true;
    return false;
  }

 inline bool jj_3_1878()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1872()) {
    jj_scanpos = xsp;
    if (jj_3_1873()) return true;
    }
    if (jj_3R_simple_value_specification_1423_5_220()) return true;
    return false;
  }

 inline bool jj_3R_delete_statement_positioned_6789_5_769()
 {
    if (jj_done) return true;
    if (jj_scan_token(DELETE)) return true;
    if (jj_scan_token(FROM)) return true;
    if (jj_3R_target_table_6796_5_877()) return true;
    return false;
  }

 inline bool jj_3_1879()
 {
    if (jj_done) return true;
    if (jj_scan_token(570)) return true;
    if (jj_3R_target_specification_1434_3_475()) return true;
    return false;
  }

 inline bool jj_3_1877()
 {
    if (jj_done) return true;
    if (jj_scan_token(LAST)) return true;
    return false;
  }

 inline bool jj_3_1880()
 {
    if (jj_done) return true;
    if (jj_3R_set_quantifier_4159_5_396()) return true;
    return false;
  }

 inline bool jj_3R_select_target_list_6783_5_817()
 {
    if (jj_done) return true;
    if (jj_3R_target_specification_1434_3_475()) return true;
    Token * xsp;
    while (true) {
      xsp = jj_scanpos;
      if (jj_3_1883()) { jj_scanpos = xsp; break; }
    }
    return false;
  }

 inline bool jj_3_1882()
 {
    if (jj_done) return true;
    if (jj_3R_table_expression_2797_5_419()) return true;
    return false;
  }

 inline bool jj_3_1881()
 {
    if (jj_done) return true;
    if (jj_scan_token(INTO)) return true;
    if (jj_3R_select_target_list_6783_5_817()) return true;
    return false;
  }

 inline bool jj_3_1876()
 {
    if (jj_done) return true;
    if (jj_scan_token(FIRST)) return true;
    return false;
  }

 inline bool jj_3R_select_statement_single_row_6775_5_767()
 {
    if (jj_done) return true;
    if (jj_scan_token(SELECT)) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1880()) jj_scanpos = xsp;
    if (jj_3R_select_list_3330_5_940()) return true;
    xsp = jj_scanpos;
    if (jj_3_1881()) jj_scanpos = xsp;
    xsp = jj_scanpos;
    if (jj_3_1882()) jj_scanpos = xsp;
    return false;
  }

 inline bool jj_3_1867()
 {
    if (jj_done) return true;
    if (jj_scan_token(OF)) return true;
    if (jj_3R_column_name_list_2942_5_191()) return true;
    return false;
  }

 inline bool jj_3R_close_statement_6769_5_766()
 {
    if (jj_done) return true;
    if (jj_scan_token(CLOSE)) return true;
    if (jj_3R_cursor_name_995_5_144()) return true;
    return false;
  }

 inline bool jj_3_1875()
 {
    if (jj_done) return true;
    if (jj_scan_token(PRIOR)) return true;
    return false;
  }

 inline bool jj_3_1869()
 {
    if (jj_done) return true;
    if (jj_scan_token(UPDATE)) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1867()) jj_scanpos = xsp;
    return false;
  }

 inline bool jj_3_1870()
 {
    if (jj_done) return true;
    if (jj_3R_fetch_orientation_6757_5_816()) return true;
    return false;
  }

 inline bool jj_3_1871()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1870()) jj_scanpos = xsp;
    if (jj_scan_token(FROM)) return true;
    return false;
  }

 inline bool jj_3R_fetch_orientation_6757_5_816()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1874()) {
    jj_scanpos = xsp;
    if (jj_3_1875()) {
    jj_scanpos = xsp;
    if (jj_3_1876()) {
    jj_scanpos = xsp;
    if (jj_3_1877()) {
    jj_scanpos = xsp;
    if (jj_3_1878()) return true;
    }
    }
    }
    }
    return false;
  }

 inline bool jj_3_1874()
 {
    if (jj_done) return true;
    if (jj_scan_token(NEXT)) return true;
    return false;
  }

 inline bool jj_3_1866()
 {
    if (jj_done) return true;
    if (jj_3R_updatability_clause_6739_5_815()) return true;
    return false;
  }

 inline bool jj_3R_fetch_statement_6751_5_765()
 {
    if (jj_done) return true;
    if (jj_scan_token(FETCH)) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1871()) jj_scanpos = xsp;
    if (jj_3R_cursor_name_995_5_144()) return true;
    if (jj_scan_token(INTO)) return true;
    return false;
  }

 inline bool jj_3_1868()
 {
    if (jj_done) return true;
    if (jj_scan_token(READ)) return true;
    if (jj_scan_token(ONLY)) return true;
    return false;
  }

 inline bool jj_3R_open_statement_6745_5_764()
 {
    if (jj_done) return true;
    if (jj_scan_token(OPEN)) return true;
    if (jj_3R_cursor_name_995_5_144()) return true;
    return false;
  }

 inline bool jj_3R_updatability_clause_6739_5_815()
 {
    if (jj_done) return true;
    if (jj_scan_token(FOR)) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1868()) {
    jj_scanpos = xsp;
    if (jj_3_1869()) return true;
    }
    return false;
  }

 inline bool jj_3R_cursor_specification_6733_5_1018()
 {
    if (jj_done) return true;
    if (jj_3R_query_expression_3399_5_547()) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1866()) jj_scanpos = xsp;
    return false;
  }

 inline bool jj_3_1865()
 {
    if (jj_done) return true;
    if (jj_scan_token(WITHOUT)) return true;
    if (jj_scan_token(RETURN)) return true;
    return false;
  }

 inline bool jj_3R_cursor_returnability_6726_5_814()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1864()) {
    jj_scanpos = xsp;
    if (jj_3_1865()) return true;
    }
    return false;
  }

 inline bool jj_3_1864()
 {
    if (jj_done) return true;
    if (jj_scan_token(WITH)) return true;
    if (jj_scan_token(RETURN)) return true;
    return false;
  }

 inline bool jj_3_1854()
 {
    if (jj_done) return true;
    if (jj_3R_cursor_scrollability_6712_5_812()) return true;
    return false;
  }

 inline bool jj_3_1863()
 {
    if (jj_done) return true;
    if (jj_scan_token(WITHOUT)) return true;
    if (jj_scan_token(HOLD)) return true;
    return false;
  }

 inline bool jj_3R_cursor_holdability_6719_5_813()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1862()) {
    jj_scanpos = xsp;
    if (jj_3_1863()) return true;
    }
    return false;
  }

 inline bool jj_3_1862()
 {
    if (jj_done) return true;
    if (jj_scan_token(WITH)) return true;
    if (jj_scan_token(HOLD)) return true;
    return false;
  }

 inline bool jj_3_1861()
 {
    if (jj_done) return true;
    if (jj_scan_token(NO)) return true;
    if (jj_scan_token(SCROLL)) return true;
    return false;
  }

 inline bool jj_3R_cursor_scrollability_6712_5_812()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1860()) {
    jj_scanpos = xsp;
    if (jj_3_1861()) return true;
    }
    return false;
  }

 inline bool jj_3_1860()
 {
    if (jj_done) return true;
    if (jj_scan_token(SCROLL)) return true;
    return false;
  }

 inline bool jj_3_1859()
 {
    if (jj_done) return true;
    if (jj_scan_token(ASENSITIVE)) return true;
    return false;
  }

 inline bool jj_3_1858()
 {
    if (jj_done) return true;
    if (jj_scan_token(INSENSITIVE)) return true;
    return false;
  }

 inline bool jj_3R_cursor_sensitivity_6704_5_811()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1857()) {
    jj_scanpos = xsp;
    if (jj_3_1858()) {
    jj_scanpos = xsp;
    if (jj_3_1859()) return true;
    }
    }
    return false;
  }

 inline bool jj_3_1857()
 {
    if (jj_done) return true;
    if (jj_scan_token(SENSITIVE)) return true;
    return false;
  }

 inline bool jj_3_346()
 {
    if (jj_done) return true;
    if (jj_scan_token(COUNT_QUOTED)) return true;
    return false;
  }

 inline bool jj_3_345()
 {
    if (jj_done) return true;
    if (jj_scan_token(344)) return true;
    return false;
  }

 inline bool jj_3_344()
 {
    if (jj_done) return true;
    if (jj_scan_token(343)) return true;
    return false;
  }

 inline bool jj_3_1856()
 {
    if (jj_done) return true;
    if (jj_3R_cursor_returnability_6726_5_814()) return true;
    return false;
  }

 inline bool jj_3_343()
 {
    if (jj_done) return true;
    if (jj_scan_token(342)) return true;
    return false;
  }

 inline bool jj_3_1855()
 {
    if (jj_done) return true;
    if (jj_3R_cursor_holdability_6719_5_813()) return true;
    return false;
  }

 inline bool jj_3_342()
 {
    if (jj_done) return true;
    if (jj_scan_token(341)) return true;
    return false;
  }

 inline bool jj_3_1853()
 {
    if (jj_done) return true;
    if (jj_3R_cursor_sensitivity_6704_5_811()) return true;
    return false;
  }

 inline bool jj_3_341()
 {
    if (jj_done) return true;
    if (jj_scan_token(340)) return true;
    return false;
  }

 inline bool jj_3R_cursor_properties_6696_5_989()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1853()) jj_scanpos = xsp;
    xsp = jj_scanpos;
    if (jj_3_1854()) jj_scanpos = xsp;
    if (jj_scan_token(CURSOR)) return true;
    xsp = jj_scanpos;
    if (jj_3_1855()) jj_scanpos = xsp;
    xsp = jj_scanpos;
    if (jj_3_1856()) jj_scanpos = xsp;
    return false;
  }

 inline bool jj_3_340()
 {
    if (jj_done) return true;
    if (jj_scan_token(REPLACE)) return true;
    return false;
  }

 inline bool jj_3_339()
 {
    if (jj_done) return true;
    if (jj_scan_token(338)) return true;
    return false;
  }

 inline bool jj_3_338()
 {
    if (jj_done) return true;
    if (jj_scan_token(LIMIT)) return true;
    return false;
  }

 inline bool jj_3_337()
 {
    if (jj_done) return true;
    if (jj_scan_token(USE)) return true;
    return false;
  }

 inline bool jj_3_336()
 {
    if (jj_done) return true;
    if (jj_scan_token(DEFAULT_)) return true;
    return false;
  }

 inline bool jj_3_335()
 {
    if (jj_done) return true;
    if (jj_scan_token(COMMENT)) return true;
    return false;
  }

 inline bool jj_3R_declare_cursor_6689_5_722()
 {
    if (jj_done) return true;
    if (jj_scan_token(DECLARE)) return true;
    if (jj_3R_cursor_name_995_5_144()) return true;
    if (jj_3R_cursor_properties_6696_5_989()) return true;
    return false;
  }

 inline bool jj_3_334()
 {
    if (jj_done) return true;
    if (jj_scan_token(YEARS)) return true;
    return false;
  }

 inline bool jj_3_333()
 {
    if (jj_done) return true;
    if (jj_scan_token(YEAR)) return true;
    return false;
  }

 inline bool jj_3_332()
 {
    if (jj_done) return true;
    if (jj_scan_token(WINDOW)) return true;
    return false;
  }

 inline bool jj_3_331()
 {
    if (jj_done) return true;
    if (jj_scan_token(VERSIONS)) return true;
    return false;
  }

 inline bool jj_3_330()
 {
    if (jj_done) return true;
    if (jj_scan_token(VERSION)) return true;
    return false;
  }

 inline bool jj_3_329()
 {
    if (jj_done) return true;
    if (jj_scan_token(VALUES)) return true;
    return false;
  }

 inline bool jj_3_1852()
 {
    if (jj_done) return true;
    if (jj_3R_get_descriptor_statement_7347_5_810()) return true;
    return false;
  }

 inline bool jj_3_328()
 {
    if (jj_done) return true;
    if (jj_scan_token(VALUE)) return true;
    return false;
  }

 inline bool jj_3_1851()
 {
    if (jj_done) return true;
    if (jj_3R_set_descriptor_statement_7437_5_809()) return true;
    return false;
  }

 inline bool jj_3_327()
 {
    if (jj_done) return true;
    if (jj_scan_token(USER)) return true;
    return false;
  }

 inline bool jj_3_1850()
 {
    if (jj_done) return true;
    if (jj_3R_deallocate_descriptor_statement_7341_5_808()) return true;
    return false;
  }

 inline bool jj_3R_SQL_descriptor_statement_6680_5_794()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1849()) {
    jj_scanpos = xsp;
    if (jj_3_1850()) {
    jj_scanpos = xsp;
    if (jj_3_1851()) {
    jj_scanpos = xsp;
    if (jj_3_1852()) return true;
    }
    }
    }
    return false;
  }

 inline bool jj_3_326()
 {
    if (jj_done) return true;
    if (jj_scan_token(UPPER)) return true;
    return false;
  }

 inline bool jj_3_1849()
 {
    if (jj_done) return true;
    if (jj_3R_allocate_descriptor_statement_7335_5_807()) return true;
    return false;
  }

 inline bool jj_3_325()
 {
    if (jj_done) return true;
    if (jj_scan_token(UPDATE)) return true;
    return false;
  }

 inline bool jj_3_324()
 {
    if (jj_done) return true;
    if (jj_scan_token(UNKNOWN)) return true;
    return false;
  }

 inline bool jj_3_323()
 {
    if (jj_done) return true;
    if (jj_scan_token(TRUNCATE)) return true;
    return false;
  }

 inline bool jj_3_322()
 {
    if (jj_done) return true;
    if (jj_scan_token(TRIGGER)) return true;
    return false;
  }

 inline bool jj_3_321()
 {
    if (jj_done) return true;
    if (jj_scan_token(TIMEZONE_MINUTE)) return true;
    return false;
  }

 inline bool jj_3_320()
 {
    if (jj_done) return true;
    if (jj_scan_token(TIMEZONE_HOUR)) return true;
    return false;
  }

 inline bool jj_3_1848()
 {
    if (jj_done) return true;
    if (jj_3R_dynamic_update_statement_positioned_7739_5_806()) return true;
    return false;
  }

 inline bool jj_3_319()
 {
    if (jj_done) return true;
    if (jj_scan_token(TIMESTAMP)) return true;
    return false;
  }

 inline bool jj_3_1847()
 {
    if (jj_done) return true;
    if (jj_3R_dynamic_delete_statement_positioned_7733_5_805()) return true;
    return false;
  }

 inline bool jj_3_318()
 {
    if (jj_done) return true;
    if (jj_scan_token(TIME)) return true;
    return false;
  }

 inline bool jj_3_1846()
 {
    if (jj_done) return true;
    if (jj_3R_dynamic_close_statement_7727_5_804()) return true;
    return false;
  }

 inline bool jj_3_317()
 {
    if (jj_done) return true;
    if (jj_scan_token(SYSTEM)) return true;
    return false;
  }

 inline bool jj_3_1845()
 {
    if (jj_done) return true;
    if (jj_3R_dynamic_fetch_statement_7715_5_803()) return true;
    return false;
  }

 inline bool jj_3_316()
 {
    if (jj_done) return true;
    if (jj_scan_token(SUM)) return true;
    return false;
  }

 inline bool jj_3_1844()
 {
    if (jj_done) return true;
    if (jj_3R_dynamic_open_statement_7709_5_802()) return true;
    return false;
  }

 inline bool jj_3R_SQL_dynamic_data_statement_6669_5_800()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1843()) {
    jj_scanpos = xsp;
    if (jj_3_1844()) {
    jj_scanpos = xsp;
    if (jj_3_1845()) {
    jj_scanpos = xsp;
    if (jj_3_1846()) {
    jj_scanpos = xsp;
    if (jj_3_1847()) {
    jj_scanpos = xsp;
    if (jj_3_1848()) return true;
    }
    }
    }
    }
    }
    return false;
  }

 inline bool jj_3_315()
 {
    if (jj_done) return true;
    if (jj_scan_token(STATIC)) return true;
    return false;
  }

 inline bool jj_3_1843()
 {
    if (jj_done) return true;
    if (jj_3R_allocate_cursor_statement_7683_5_801()) return true;
    return false;
  }

 inline bool jj_3_314()
 {
    if (jj_done) return true;
    if (jj_scan_token(START)) return true;
    return false;
  }

 inline bool jj_3_313()
 {
    if (jj_done) return true;
    if (jj_scan_token(SQL)) return true;
    return false;
  }

 inline bool jj_3_312()
 {
    if (jj_done) return true;
    if (jj_scan_token(SESSION_USER)) return true;
    return false;
  }

 inline bool jj_3_311()
 {
    if (jj_done) return true;
    if (jj_scan_token(SECONDS)) return true;
    return false;
  }

 inline bool jj_3_310()
 {
    if (jj_done) return true;
    if (jj_scan_token(SECOND)) return true;
    return false;
  }

 inline bool jj_3_309()
 {
    if (jj_done) return true;
    if (jj_scan_token(SEARCH)) return true;
    return false;
  }

 inline bool jj_3_1842()
 {
    if (jj_done) return true;
    if (jj_3R_SQL_dynamic_data_statement_6669_5_800()) return true;
    return false;
  }

 inline bool jj_3_308()
 {
    if (jj_done) return true;
    if (jj_scan_token(SCOPE)) return true;
    return false;
  }

 inline bool jj_3_1841()
 {
    if (jj_done) return true;
    if (jj_3R_execute_immediate_statement_7669_5_799()) return true;
    return false;
  }

 inline bool jj_3_307()
 {
    if (jj_done) return true;
    if (jj_scan_token(SAVEPOINT)) return true;
    return false;
  }

 inline bool jj_3_1840()
 {
    if (jj_done) return true;
    if (jj_3R_execute_statement_7651_5_798()) return true;
    return false;
  }

 inline bool jj_3_306()
 {
    if (jj_done) return true;
    if (jj_scan_token(ROWS)) return true;
    return false;
  }

 inline bool jj_3_1839()
 {
    if (jj_done) return true;
    if (jj_3R_describe_statement_7562_5_797()) return true;
    return false;
  }

 inline bool jj_3_305()
 {
    if (jj_done) return true;
    if (jj_scan_token(ROW_NUMBER)) return true;
    return false;
  }

 inline bool jj_3_1838()
 {
    if (jj_done) return true;
    if (jj_3R_deallocate_prepared_statement_7556_5_796()) return true;
    return false;
  }

 inline bool jj_3_304()
 {
    if (jj_done) return true;
    if (jj_scan_token(ROW)) return true;
    return false;
  }

 inline bool jj_3_1837()
 {
    if (jj_done) return true;
    if (jj_3R_prepare_statement_7463_5_795()) return true;
    return false;
  }

 inline bool jj_3R_SQL_dynamic_statement_6657_5_736()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1836()) {
    jj_scanpos = xsp;
    if (jj_3_1837()) {
    jj_scanpos = xsp;
    if (jj_3_1838()) {
    jj_scanpos = xsp;
    if (jj_3_1839()) {
    jj_scanpos = xsp;
    if (jj_3_1840()) {
    jj_scanpos = xsp;
    if (jj_3_1841()) {
    jj_scanpos = xsp;
    if (jj_3_1842()) return true;
    }
    }
    }
    }
    }
    }
    return false;
  }

 inline bool jj_3_303()
 {
    if (jj_done) return true;
    if (jj_scan_token(ROLLUP)) return true;
    return false;
  }

 inline bool jj_3_1836()
 {
    if (jj_done) return true;
    if (jj_3R_SQL_descriptor_statement_6680_5_794()) return true;
    return false;
  }

 inline bool jj_3_302()
 {
    if (jj_done) return true;
    if (jj_scan_token(RETURNS)) return true;
    return false;
  }

 inline bool jj_3_301()
 {
    if (jj_done) return true;
    if (jj_scan_token(RESULT)) return true;
    return false;
  }

 inline bool jj_3_300()
 {
    if (jj_done) return true;
    if (jj_scan_token(RELEASE)) return true;
    return false;
  }

 inline bool jj_3_299()
 {
    if (jj_done) return true;
    if (jj_scan_token(REFERENCES)) return true;
    return false;
  }

 inline bool jj_3_298()
 {
    if (jj_done) return true;
    if (jj_scan_token(REF)) return true;
    return false;
  }

 inline bool jj_3R_SQL_diagnostics_statement_6651_5_735()
 {
    if (jj_done) return true;
    if (jj_3R_get_diagnostics_statement_7809_5_991()) return true;
    return false;
  }

 inline bool jj_3_297()
 {
    if (jj_done) return true;
    if (jj_scan_token(READS)) return true;
    return false;
  }

 inline bool jj_3_296()
 {
    if (jj_done) return true;
    if (jj_scan_token(RANK)) return true;
    return false;
  }

 inline bool jj_3_295()
 {
    if (jj_done) return true;
    if (jj_scan_token(RANGE)) return true;
    return false;
  }

 inline bool jj_3_294()
 {
    if (jj_done) return true;
    if (jj_scan_token(PRECISION)) return true;
    return false;
  }

 inline bool jj_3_293()
 {
    if (jj_done) return true;
    if (jj_scan_token(POWER)) return true;
    return false;
  }

 inline bool jj_3_292()
 {
    if (jj_done) return true;
    if (jj_scan_token(POSITION)) return true;
    return false;
  }

 inline bool jj_3_291()
 {
    if (jj_done) return true;
    if (jj_scan_token(PARTITION)) return true;
    return false;
  }

 inline bool jj_3_1835()
 {
    if (jj_done) return true;
    if (jj_3R_set_session_collation_statement_7322_5_793()) return true;
    return false;
  }

 inline bool jj_3_290()
 {
    if (jj_done) return true;
    if (jj_scan_token(OPEN)) return true;
    return false;
  }

 inline bool jj_3_1834()
 {
    if (jj_done) return true;
    if (jj_3R_set_transform_group_statement_7309_5_792()) return true;
    return false;
  }

 inline bool jj_3_289()
 {
    if (jj_done) return true;
    if (jj_scan_token(OLD)) return true;
    return false;
  }

 inline bool jj_3_1833()
 {
    if (jj_done) return true;
    if (jj_3R_set_path_statement_7297_5_791()) return true;
    return false;
  }

 inline bool jj_3_288()
 {
    if (jj_done) return true;
    if (jj_scan_token(OFFSET)) return true;
    return false;
  }

 inline bool jj_3_1832()
 {
    if (jj_done) return true;
    if (jj_3R_set_names_statement_7285_5_790()) return true;
    return false;
  }

 inline bool jj_3_287()
 {
    if (jj_done) return true;
    if (jj_scan_token(OCCURRENCE)) return true;
    return false;
  }

 inline bool jj_3_1831()
 {
    if (jj_done) return true;
    if (jj_3R_set_schema_statement_7273_5_789()) return true;
    return false;
  }

 inline bool jj_3_286()
 {
    if (jj_done) return true;
    if (jj_scan_token(NONE)) return true;
    return false;
  }

 inline bool jj_3_1830()
 {
    if (jj_done) return true;
    if (jj_3R_set_catalog_statement_7261_5_788()) return true;
    return false;
  }

 inline bool jj_3_285()
 {
    if (jj_done) return true;
    if (jj_scan_token(NEW)) return true;
    return false;
  }

 inline bool jj_3_1829()
 {
    if (jj_done) return true;
    if (jj_3R_set_session_characteristics_statement_7205_5_787()) return true;
    return false;
  }

 inline bool jj_3_284()
 {
    if (jj_done) return true;
    if (jj_scan_token(NAME)) return true;
    return false;
  }

 inline bool jj_3_1828()
 {
    if (jj_done) return true;
    if (jj_3R_set_local_time_zone_statement_7248_5_786()) return true;
    return false;
  }

 inline bool jj_3_283()
 {
    if (jj_done) return true;
    if (jj_scan_token(MONTHS)) return true;
    return false;
  }

 inline bool jj_3_1827()
 {
    if (jj_done) return true;
    if (jj_3R_set_role_statement_7235_5_785()) return true;
    return false;
  }

 inline bool jj_3R_SQL_session_statement_6636_5_734()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1826()) {
    jj_scanpos = xsp;
    if (jj_3_1827()) {
    jj_scanpos = xsp;
    if (jj_3_1828()) {
    jj_scanpos = xsp;
    if (jj_3_1829()) {
    jj_scanpos = xsp;
    if (jj_3_1830()) {
    jj_scanpos = xsp;
    if (jj_3_1831()) {
    jj_scanpos = xsp;
    if (jj_3_1832()) {
    jj_scanpos = xsp;
    if (jj_3_1833()) {
    jj_scanpos = xsp;
    if (jj_3_1834()) {
    jj_scanpos = xsp;
    if (jj_3_1835()) return true;
    }
    }
    }
    }
    }
    }
    }
    }
    }
    return false;
  }

 inline bool jj_3_282()
 {
    if (jj_done) return true;
    if (jj_scan_token(MONTH)) return true;
    return false;
  }

 inline bool jj_3_1826()
 {
    if (jj_done) return true;
    if (jj_3R_set_session_user_identifier_statement_7229_5_784()) return true;
    return false;
  }

 inline bool jj_3_281()
 {
    if (jj_done) return true;
    if (jj_scan_token(MODULE)) return true;
    return false;
  }

 inline bool jj_3_280()
 {
    if (jj_done) return true;
    if (jj_scan_token(MOD)) return true;
    return false;
  }

 inline bool jj_3_279()
 {
    if (jj_done) return true;
    if (jj_scan_token(MINUTES)) return true;
    return false;
  }

 inline bool jj_3_278()
 {
    if (jj_done) return true;
    if (jj_scan_token(MINUTE)) return true;
    return false;
  }

 inline bool jj_3_277()
 {
    if (jj_done) return true;
    if (jj_scan_token(MIN)) return true;
    return false;
  }

 inline bool jj_3_276()
 {
    if (jj_done) return true;
    if (jj_scan_token(METHOD)) return true;
    return false;
  }

 inline bool jj_3_1825()
 {
    if (jj_done) return true;
    if (jj_3R_disconnect_statement_7191_5_783()) return true;
    return false;
  }

 inline bool jj_3_275()
 {
    if (jj_done) return true;
    if (jj_scan_token(MERGE)) return true;
    return false;
  }

 inline bool jj_3_1824()
 {
    if (jj_done) return true;
    if (jj_3R_set_connection_statement_7178_5_782()) return true;
    return false;
  }

 inline bool jj_3R_SQL_connection_statement_6628_5_733()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1823()) {
    jj_scanpos = xsp;
    if (jj_3_1824()) {
    jj_scanpos = xsp;
    if (jj_3_1825()) return true;
    }
    }
    return false;
  }

 inline bool jj_3_274()
 {
    if (jj_done) return true;
    if (jj_scan_token(MAX)) return true;
    return false;
  }

 inline bool jj_3_1823()
 {
    if (jj_done) return true;
    if (jj_3R_connect_statement_7165_5_781()) return true;
    return false;
  }

 inline bool jj_3_273()
 {
    if (jj_done) return true;
    if (jj_scan_token(LOWER)) return true;
    return false;
  }

 inline bool jj_3_272()
 {
    if (jj_done) return true;
    if (jj_scan_token(LOCAL)) return true;
    return false;
  }

 inline bool jj_3_271()
 {
    if (jj_done) return true;
    if (jj_scan_token(LEAD)) return true;
    return false;
  }

 inline bool jj_3_270()
 {
    if (jj_done) return true;
    if (jj_scan_token(LANGUAGE)) return true;
    return false;
  }

 inline bool jj_3_269()
 {
    if (jj_done) return true;
    if (jj_scan_token(INTERVAL)) return true;
    return false;
  }

 inline bool jj_3_268()
 {
    if (jj_done) return true;
    if (jj_scan_token(INTERSECTION)) return true;
    return false;
  }

 inline bool jj_3_1822()
 {
    if (jj_done) return true;
    if (jj_3R_rollback_statement_7153_5_780()) return true;
    return false;
  }

 inline bool jj_3_267()
 {
    if (jj_done) return true;
    if (jj_scan_token(INDICATOR)) return true;
    return false;
  }

 inline bool jj_3_1821()
 {
    if (jj_done) return true;
    if (jj_3R_commit_statement_7147_5_779()) return true;
    return false;
  }

 inline bool jj_3_266()
 {
    if (jj_done) return true;
    if (jj_scan_token(IDENTITY)) return true;
    return false;
  }

 inline bool jj_3_1820()
 {
    if (jj_done) return true;
    if (jj_3R_release_savepoint_statement_7141_5_778()) return true;
    return false;
  }

 inline bool jj_3_265()
 {
    if (jj_done) return true;
    if (jj_scan_token(HOURS)) return true;
    return false;
  }

 inline bool jj_3_1819()
 {
    if (jj_done) return true;
    if (jj_3R_savepoint_statement_7129_5_777()) return true;
    return false;
  }

 inline bool jj_3_264()
 {
    if (jj_done) return true;
    if (jj_scan_token(HOUR)) return true;
    return false;
  }

 inline bool jj_3_1818()
 {
    if (jj_done) return true;
    if (jj_3R_set_constraints_mode_statement_7116_5_776()) return true;
    return false;
  }

 inline bool jj_3_263()
 {
    if (jj_done) return true;
    if (jj_scan_token(HOLD)) return true;
    return false;
  }

 inline bool jj_3_1817()
 {
    if (jj_done) return true;
    if (jj_3R_set_transaction_statement_7068_5_775()) return true;
    return false;
  }

 inline bool jj_3R_SQL_transaction_statement_6616_5_732()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1816()) {
    jj_scanpos = xsp;
    if (jj_3_1817()) {
    jj_scanpos = xsp;
    if (jj_3_1818()) {
    jj_scanpos = xsp;
    if (jj_3_1819()) {
    jj_scanpos = xsp;
    if (jj_3_1820()) {
    jj_scanpos = xsp;
    if (jj_3_1821()) {
    jj_scanpos = xsp;
    if (jj_3_1822()) return true;
    }
    }
    }
    }
    }
    }
    return false;
  }

 inline bool jj_3_262()
 {
    if (jj_done) return true;
    if (jj_scan_token(GLOBAL)) return true;
    return false;
  }

 inline bool jj_3_1816()
 {
    if (jj_done) return true;
    if (jj_3R_start_transaction_statement_7062_5_774()) return true;
    return false;
  }

 inline bool jj_3_261()
 {
    if (jj_done) return true;
    if (jj_scan_token(FUNCTION)) return true;
    return false;
  }

 inline bool jj_3_260()
 {
    if (jj_done) return true;
    if (jj_scan_token(FREE)) return true;
    return false;
  }

 inline bool jj_3_259()
 {
    if (jj_done) return true;
    if (jj_scan_token(FLOOR)) return true;
    return false;
  }

 inline bool jj_3_258()
 {
    if (jj_done) return true;
    if (jj_scan_token(FILTER)) return true;
    return false;
  }

 inline bool jj_3_257()
 {
    if (jj_done) return true;
    if (jj_scan_token(EXTERNAL)) return true;
    return false;
  }

 inline bool jj_3_256()
 {
    if (jj_done) return true;
    if (jj_scan_token(EXP)) return true;
    return false;
  }

 inline bool jj_3_1815()
 {
    if (jj_done) return true;
    if (jj_3R_return_statement_7049_5_773()) return true;
    return false;
  }

 inline bool jj_3R_SQL_control_statement_6609_5_731()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1814()) {
    jj_scanpos = xsp;
    if (jj_3_1815()) return true;
    }
    return false;
  }

 inline bool jj_3_255()
 {
    if (jj_done) return true;
    if (jj_scan_token(DYNAMIC)) return true;
    return false;
  }

 inline bool jj_3_1814()
 {
    if (jj_done) return true;
    if (jj_3R_call_statement_7043_5_772()) return true;
    return false;
  }

 inline bool jj_3_254()
 {
    if (jj_done) return true;
    if (jj_scan_token(DEC)) return true;
    return false;
  }

 inline bool jj_3_253()
 {
    if (jj_done) return true;
    if (jj_scan_token(DAYS)) return true;
    return false;
  }

 inline bool jj_3_252()
 {
    if (jj_done) return true;
    if (jj_scan_token(DAY)) return true;
    return false;
  }

 inline bool jj_3_251()
 {
    if (jj_done) return true;
    if (jj_scan_token(DATE)) return true;
    return false;
  }

 inline bool jj_3_250()
 {
    if (jj_done) return true;
    if (jj_scan_token(CYCLE)) return true;
    return false;
  }

 inline bool jj_3_249()
 {
    if (jj_done) return true;
    if (jj_scan_token(CURSOR)) return true;
    return false;
  }

 inline bool jj_3_1813()
 {
    if (jj_done) return true;
    if (jj_3R_merge_statement_6878_5_385()) return true;
    return false;
  }

 inline bool jj_3_248()
 {
    if (jj_done) return true;
    if (jj_scan_token(CURRENT_USER)) return true;
    return false;
  }

 inline bool jj_3_1812()
 {
    if (jj_done) return true;
    if (jj_3R_truncate_table_statement_6810_5_771()) return true;
    return false;
  }

 inline bool jj_3_247()
 {
    if (jj_done) return true;
    if (jj_scan_token(CURRENT_TRANSFORM_GROUP_FOR_TYPE)) return true;
    return false;
  }

 inline bool jj_3_1811()
 {
    if (jj_done) return true;
    if (jj_3R_update_statement_searched_6963_5_386()) return true;
    return false;
  }

 inline bool jj_3_246()
 {
    if (jj_done) return true;
    if (jj_scan_token(CURRENT_TIMESTAMP)) return true;
    return false;
  }

 inline bool jj_3_1810()
 {
    if (jj_done) return true;
    if (jj_3R_update_statement_positioned_6955_5_770()) return true;
    return false;
  }

 inline bool jj_3_245()
 {
    if (jj_done) return true;
    if (jj_scan_token(CURRENT_TIME)) return true;
    return false;
  }

 inline bool jj_3_1809()
 {
    if (jj_done) return true;
    if (jj_3R_insert_statement_6823_5_384()) return true;
    return false;
  }

 inline bool jj_3_244()
 {
    if (jj_done) return true;
    if (jj_scan_token(CURRENT_SCHEMA)) return true;
    return false;
  }

 inline bool jj_3_1808()
 {
    if (jj_done) return true;
    if (jj_3R_delete_statement_searched_6803_5_383()) return true;
    return false;
  }

 inline bool jj_3R_SQL_data_change_statement_6597_5_768()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1807()) {
    jj_scanpos = xsp;
    if (jj_3_1808()) {
    jj_scanpos = xsp;
    if (jj_3_1809()) {
    jj_scanpos = xsp;
    if (jj_3_1810()) {
    jj_scanpos = xsp;
    if (jj_3_1811()) {
    jj_scanpos = xsp;
    if (jj_3_1812()) {
    jj_scanpos = xsp;
    if (jj_3_1813()) return true;
    }
    }
    }
    }
    }
    }
    return false;
  }

 inline bool jj_3_243()
 {
    if (jj_done) return true;
    if (jj_scan_token(CURRENT_ROLE)) return true;
    return false;
  }

 inline bool jj_3_1807()
 {
    if (jj_done) return true;
    if (jj_3R_delete_statement_positioned_6789_5_769()) return true;
    return false;
  }

 inline bool jj_3_242()
 {
    if (jj_done) return true;
    if (jj_scan_token(CURRENT_PATH)) return true;
    return false;
  }

 inline bool jj_3_241()
 {
    if (jj_done) return true;
    if (jj_scan_token(CURRENT_DEFAULT_TRANSFORM_GROUP)) return true;
    return false;
  }

 inline bool jj_3_240()
 {
    if (jj_done) return true;
    if (jj_scan_token(CURRENT_DATE)) return true;
    return false;
  }

 inline bool jj_3_239()
 {
    if (jj_done) return true;
    if (jj_scan_token(CURRENT_CATALOG)) return true;
    return false;
  }

 inline bool jj_3_238()
 {
    if (jj_done) return true;
    if (jj_scan_token(CURRENT)) return true;
    return false;
  }

 inline bool jj_3_237()
 {
    if (jj_done) return true;
    if (jj_scan_token(CUBE)) return true;
    return false;
  }

 inline bool jj_3_236()
 {
    if (jj_done) return true;
    if (jj_scan_token(COUNT)) return true;
    return false;
  }

 inline bool jj_3_235()
 {
    if (jj_done) return true;
    if (jj_scan_token(CONDITION)) return true;
    return false;
  }

 inline bool jj_3_234()
 {
    if (jj_done) return true;
    if (jj_scan_token(COLUMN)) return true;
    return false;
  }

 inline bool jj_3_233()
 {
    if (jj_done) return true;
    if (jj_scan_token(CLOSE)) return true;
    return false;
  }

 inline bool jj_3_1806()
 {
    if (jj_done) return true;
    if (jj_3R_SQL_data_change_statement_6597_5_768()) return true;
    return false;
  }

 inline bool jj_3_232()
 {
    if (jj_done) return true;
    if (jj_scan_token(CARDINALITY)) return true;
    return false;
  }

 inline bool jj_3_1805()
 {
    if (jj_done) return true;
    if (jj_3R_select_statement_single_row_6775_5_767()) return true;
    return false;
  }

 inline bool jj_3_231()
 {
    if (jj_done) return true;
    if (jj_scan_token(BOTH)) return true;
    return false;
  }

 inline bool jj_3_1804()
 {
    if (jj_done) return true;
    if (jj_3R_close_statement_6769_5_766()) return true;
    return false;
  }

 inline bool jj_3_230()
 {
    if (jj_done) return true;
    if (jj_scan_token(BLOB)) return true;
    return false;
  }

 inline bool jj_3_1803()
 {
    if (jj_done) return true;
    if (jj_3R_fetch_statement_6751_5_765()) return true;
    return false;
  }

 inline bool jj_3R_SQL_data_statement_6583_5_730()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1802()) {
    jj_scanpos = xsp;
    if (jj_3_1803()) {
    jj_scanpos = xsp;
    if (jj_3_1804()) {
    jj_scanpos = xsp;
    if (jj_3_1805()) {
    jj_scanpos = xsp;
    if (jj_3_1806()) return true;
    }
    }
    }
    }
    return false;
  }

 inline bool jj_3_229()
 {
    if (jj_done) return true;
    if (jj_scan_token(AVG)) return true;
    return false;
  }

 inline bool jj_3_1802()
 {
    if (jj_done) return true;
    if (jj_3R_open_statement_6745_5_764()) return true;
    return false;
  }

 inline bool jj_3_228()
 {
    if (jj_done) return true;
    if (jj_scan_token(AT)) return true;
    return false;
  }

 inline bool jj_3_227()
 {
    if (jj_done) return true;
    if (jj_scan_token(ARRAY_AGG)) return true;
    return false;
  }

 inline bool jj_3_226()
 {
    if (jj_done) return true;
    if (jj_scan_token(ALL)) return true;
    return false;
  }

 inline bool jj_3_225()
 {
    if (jj_done) return true;
    if (jj_scan_token(ABS)) return true;
    return false;
  }

 inline bool jj_3_1801()
 {
    if (jj_done) return true;
    if (jj_3R_drop_sequence_generator_statement_6260_5_763()) return true;
    return false;
  }

 inline bool jj_3_1800()
 {
    if (jj_done) return true;
    if (jj_3R_alter_sequence_generator_statement_6229_5_762()) return true;
    return false;
  }

 inline bool jj_3_1799()
 {
    if (jj_done) return true;
    if (jj_3R_drop_transform_statement_6105_5_761()) return true;
    return false;
  }

 inline bool jj_3_224()
 {
    if (jj_done) return true;
    if (jj_scan_token(ZONE)) return true;
    return false;
  }

 inline bool jj_3_1798()
 {
    if (jj_done) return true;
    if (jj_3R_alter_transform_statement_6059_5_760()) return true;
    return false;
  }

 inline bool jj_3_223()
 {
    if (jj_done) return true;
    if (jj_scan_token(WRITE)) return true;
    return false;
  }

 inline bool jj_3_1797()
 {
    if (jj_done) return true;
    if (jj_3R_drop_user_defined_ordering_statement_6003_5_759()) return true;
    return false;
  }

 inline bool jj_3_222()
 {
    if (jj_done) return true;
    if (jj_scan_token(WORK)) return true;
    return false;
  }

 inline bool jj_3_1796()
 {
    if (jj_done) return true;
    if (jj_3R_drop_data_type_statement_5564_5_758()) return true;
    return false;
  }

 inline bool jj_3_221()
 {
    if (jj_done) return true;
    if (jj_scan_token(VIEW)) return true;
    return false;
  }

 inline bool jj_3_1795()
 {
    if (jj_done) return true;
    if (jj_3R_alter_type_statement_5511_5_757()) return true;
    return false;
  }

 inline bool jj_3_220()
 {
    if (jj_done) return true;
    if (jj_scan_token(USER_DEFINED_TYPE_SCHEMA)) return true;
    return false;
  }

 inline bool jj_3_1794()
 {
    if (jj_done) return true;
    if (jj_3R_drop_trigger_statement_5296_5_756()) return true;
    return false;
  }

 inline bool jj_3_219()
 {
    if (jj_done) return true;
    if (jj_scan_token(USER_DEFINED_TYPE_NAME)) return true;
    return false;
  }

 inline bool jj_3_1793()
 {
    if (jj_done) return true;
    if (jj_3R_drop_assertion_statement_5224_5_755()) return true;
    return false;
  }

 inline bool jj_3_218()
 {
    if (jj_done) return true;
    if (jj_scan_token(USER_DEFINED_TYPE_CODE)) return true;
    return false;
  }

 inline bool jj_3_1792()
 {
    if (jj_done) return true;
    if (jj_3R_drop_transliteration_statement_5210_5_754()) return true;
    return false;
  }

 inline bool jj_3_217()
 {
    if (jj_done) return true;
    if (jj_scan_token(USER_DEFINED_TYPE_CATALOG)) return true;
    return false;
  }

 inline bool jj_3_1791()
 {
    if (jj_done) return true;
    if (jj_3R_drop_collation_statement_5171_5_753()) return true;
    return false;
  }

 inline bool jj_3_216()
 {
    if (jj_done) return true;
    if (jj_scan_token(USAGE)) return true;
    return false;
  }

 inline bool jj_3_1790()
 {
    if (jj_done) return true;
    if (jj_3R_drop_character_set_statement_5151_5_752()) return true;
    return false;
  }

 inline bool jj_3_215()
 {
    if (jj_done) return true;
    if (jj_scan_token(UNNAMED)) return true;
    return false;
  }

 inline bool jj_3_1789()
 {
    if (jj_done) return true;
    if (jj_3R_drop_domain_statement_5132_5_751()) return true;
    return false;
  }

 inline bool jj_3_214()
 {
    if (jj_done) return true;
    if (jj_scan_token(UNDER)) return true;
    return false;
  }

 inline bool jj_3_1788()
 {
    if (jj_done) return true;
    if (jj_3R_alter_domain_statement_5093_5_750()) return true;
    return false;
  }

 inline bool jj_3_213()
 {
    if (jj_done) return true;
    if (jj_scan_token(UNCOMMITTED)) return true;
    return false;
  }

 inline bool jj_3_1787()
 {
    if (jj_done) return true;
    if (jj_3R_drop_role_statement_6365_5_749()) return true;
    return false;
  }

 inline bool jj_3_212()
 {
    if (jj_done) return true;
    if (jj_scan_token(UNBOUNDED)) return true;
    return false;
  }

 inline bool jj_3_1786()
 {
    if (jj_done) return true;
    if (jj_3R_revoke_statement_6371_5_748()) return true;
    return false;
  }

 inline bool jj_3_211()
 {
    if (jj_done) return true;
    if (jj_scan_token(TYPE)) return true;
    return false;
  }

 inline bool jj_3_1785()
 {
    if (jj_done) return true;
    if (jj_3R_drop_user_defined_cast_statement_5933_5_747()) return true;
    return false;
  }

 inline bool jj_3_210()
 {
    if (jj_done) return true;
    if (jj_scan_token(TRY_CAST)) return true;
    return false;
  }

 inline bool jj_3_1784()
 {
    if (jj_done) return true;
    if (jj_3R_drop_routine_statement_5901_5_746()) return true;
    return false;
  }

 inline bool jj_3_209()
 {
    if (jj_done) return true;
    if (jj_scan_token(TRIGGER_SCHEMA)) return true;
    return false;
  }

 inline bool jj_3_1783()
 {
    if (jj_done) return true;
    if (jj_3R_alter_routine_statement_5871_5_745()) return true;
    return false;
  }

 inline bool jj_3_208()
 {
    if (jj_done) return true;
    if (jj_scan_token(TRIGGER_NAME)) return true;
    return false;
  }

 inline bool jj_3_1782()
 {
    if (jj_done) return true;
    if (jj_3R_drop_view_statement_5071_5_744()) return true;
    return false;
  }

 inline bool jj_3_207()
 {
    if (jj_done) return true;
    if (jj_scan_token(TRIGGER_CATALOG)) return true;
    return false;
  }

 inline bool jj_3_1781()
 {
    if (jj_done) return true;
    if (jj_3R_drop_table_statement_4995_5_743()) return true;
    return false;
  }

 inline bool jj_3_206()
 {
    if (jj_done) return true;
    if (jj_scan_token(TRANSFORMS)) return true;
    return false;
  }

 inline bool jj_3_1780()
 {
    if (jj_done) return true;
    if (jj_3R_alter_table_statement_4814_5_742()) return true;
    return false;
  }

 inline bool jj_3R_SQL_schema_manipulation_statement_6555_5_738()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1779()) {
    jj_scanpos = xsp;
    if (jj_3_1780()) {
    jj_scanpos = xsp;
    if (jj_3_1781()) {
    jj_scanpos = xsp;
    if (jj_3_1782()) {
    jj_scanpos = xsp;
    if (jj_3_1783()) {
    jj_scanpos = xsp;
    if (jj_3_1784()) {
    jj_scanpos = xsp;
    if (jj_3_1785()) {
    jj_scanpos = xsp;
    if (jj_3_1786()) {
    jj_scanpos = xsp;
    if (jj_3_1787()) {
    jj_scanpos = xsp;
    if (jj_3_1788()) {
    jj_scanpos = xsp;
    if (jj_3_1789()) {
    jj_scanpos = xsp;
    if (jj_3_1790()) {
    jj_scanpos = xsp;
    if (jj_3_1791()) {
    jj_scanpos = xsp;
    if (jj_3_1792()) {
    jj_scanpos = xsp;
    if (jj_3_1793()) {
    jj_scanpos = xsp;
    if (jj_3_1794()) {
    jj_scanpos = xsp;
    if (jj_3_1795()) {
    jj_scanpos = xsp;
    if (jj_3_1796()) {
    jj_scanpos = xsp;
    if (jj_3_1797()) {
    jj_scanpos = xsp;
    if (jj_3_1798()) {
    jj_scanpos = xsp;
    if (jj_3_1799()) {
    jj_scanpos = xsp;
    if (jj_3_1800()) {
    jj_scanpos = xsp;
    if (jj_3_1801()) return true;
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    return false;
  }

 inline bool jj_3_205()
 {
    if (jj_done) return true;
    if (jj_scan_token(TRANSFORM)) return true;
    return false;
  }

 inline bool jj_3_1779()
 {
    if (jj_done) return true;
    if (jj_3R_drop_schema_statement_4360_5_741()) return true;
    return false;
  }

 inline bool jj_3_204()
 {
    if (jj_done) return true;
    if (jj_scan_token(TRANSACTIONS_ROLLED_BACK)) return true;
    return false;
  }

 inline bool jj_3_203()
 {
    if (jj_done) return true;
    if (jj_scan_token(TRANSACTIONS_COMMITTED)) return true;
    return false;
  }

 inline bool jj_3_202()
 {
    if (jj_done) return true;
    if (jj_scan_token(TRANSACTION_ACTIVE)) return true;
    return false;
  }

 inline bool jj_3_201()
 {
    if (jj_done) return true;
    if (jj_scan_token(TRANSACTION)) return true;
    return false;
  }

 inline bool jj_3_200()
 {
    if (jj_done) return true;
    if (jj_scan_token(TOP_LEVEL_COUNT)) return true;
    return false;
  }

 inline bool jj_3_199()
 {
    if (jj_done) return true;
    if (jj_scan_token(TIES)) return true;
    return false;
  }

 inline bool jj_3_1778()
 {
    if (jj_done) return true;
    if (jj_3R_sequence_generator_definition_6125_5_517()) return true;
    return false;
  }

 inline bool jj_3_198()
 {
    if (jj_done) return true;
    if (jj_scan_token(TEMPORARY)) return true;
    return false;
  }

 inline bool jj_3_1777()
 {
    if (jj_done) return true;
    if (jj_3R_transform_definition_6009_5_515()) return true;
    return false;
  }

 inline bool jj_3_197()
 {
    if (jj_done) return true;
    if (jj_scan_token(TABLE_NAME)) return true;
    return false;
  }

 inline bool jj_3_1776()
 {
    if (jj_done) return true;
    if (jj_3R_user_defined_ordering_definition_5940_5_514()) return true;
    return false;
  }

 inline bool jj_3_196()
 {
    if (jj_done) return true;
    if (jj_scan_token(T)) return true;
    return false;
  }

 inline bool jj_3_1775()
 {
    if (jj_done) return true;
    if (jj_3R_user_defined_cast_definition_5907_5_513()) return true;
    return false;
  }

 inline bool jj_3_195()
 {
    if (jj_done) return true;
    if (jj_scan_token(SUBCLASS_ORIGIN)) return true;
    return false;
  }

 inline bool jj_3_1774()
 {
    if (jj_done) return true;
    if (jj_3R_user_defined_type_definition_5302_5_512()) return true;
    return false;
  }

 inline bool jj_3_194()
 {
    if (jj_done) return true;
    if (jj_scan_token(STYLE)) return true;
    return false;
  }

 inline bool jj_3_1773()
 {
    if (jj_done) return true;
    if (jj_3R_trigger_definition_5230_5_511()) return true;
    return false;
  }

 inline bool jj_3_193()
 {
    if (jj_done) return true;
    if (jj_scan_token(STRUCTURE)) return true;
    return false;
  }

 inline bool jj_3_1772()
 {
    if (jj_done) return true;
    if (jj_3R_assertion_definition_5216_5_510()) return true;
    return false;
  }

 inline bool jj_3_192()
 {
    if (jj_done) return true;
    if (jj_scan_token(STATEMENT)) return true;
    return false;
  }

 inline bool jj_3_1771()
 {
    if (jj_done) return true;
    if (jj_3R_transliteration_definition_5177_5_509()) return true;
    return false;
  }

 inline bool jj_3_191()
 {
    if (jj_done) return true;
    if (jj_scan_token(STATE)) return true;
    return false;
  }

 inline bool jj_3_1770()
 {
    if (jj_done) return true;
    if (jj_3R_collation_definition_5157_5_508()) return true;
    return false;
  }

 inline bool jj_3_190()
 {
    if (jj_done) return true;
    if (jj_scan_token(SPECIFIC_NAME)) return true;
    return false;
  }

 inline bool jj_3_1769()
 {
    if (jj_done) return true;
    if (jj_3R_character_set_definition_5138_5_507()) return true;
    return false;
  }

 inline bool jj_3_189()
 {
    if (jj_done) return true;
    if (jj_scan_token(SPACE)) return true;
    return false;
  }

 inline bool jj_3_1768()
 {
    if (jj_done) return true;
    if (jj_3R_domain_definition_5077_5_506()) return true;
    return false;
  }

 inline bool jj_3_188()
 {
    if (jj_done) return true;
    if (jj_scan_token(SOURCE)) return true;
    return false;
  }

 inline bool jj_3_1767()
 {
    if (jj_done) return true;
    if (jj_3R_role_definition_6350_5_519()) return true;
    return false;
  }

 inline bool jj_3_187()
 {
    if (jj_done) return true;
    if (jj_scan_token(SIZE)) return true;
    return false;
  }

 inline bool jj_3_1766()
 {
    if (jj_done) return true;
    if (jj_3R_grant_statement_6266_5_518()) return true;
    return false;
  }

 inline bool jj_3_186()
 {
    if (jj_done) return true;
    if (jj_scan_token(SIMPLE)) return true;
    return false;
  }

 inline bool jj_3_1765()
 {
    if (jj_done) return true;
    if (jj_3R_SQL_invoked_routine_5570_5_740()) return true;
    return false;
  }

 inline bool jj_3_185()
 {
    if (jj_done) return true;
    if (jj_scan_token(SETS)) return true;
    return false;
  }

 inline bool jj_3_1764()
 {
    if (jj_done) return true;
    if (jj_3R_view_definition_5004_5_505()) return true;
    return false;
  }

 inline bool jj_3_184()
 {
    if (jj_done) return true;
    if (jj_scan_token(SESSION)) return true;
    return false;
  }

 inline bool jj_3_1763()
 {
    if (jj_done) return true;
    if (jj_3R_table_definition_4373_5_504()) return true;
    return false;
  }

 inline bool jj_3R_SQL_schema_definition_statement_6533_5_737()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1762()) {
    jj_scanpos = xsp;
    if (jj_3_1763()) {
    jj_scanpos = xsp;
    if (jj_3_1764()) {
    jj_scanpos = xsp;
    if (jj_3_1765()) {
    jj_scanpos = xsp;
    if (jj_3_1766()) {
    jj_scanpos = xsp;
    if (jj_3_1767()) {
    jj_scanpos = xsp;
    if (jj_3_1768()) {
    jj_scanpos = xsp;
    if (jj_3_1769()) {
    jj_scanpos = xsp;
    if (jj_3_1770()) {
    jj_scanpos = xsp;
    if (jj_3_1771()) {
    jj_scanpos = xsp;
    if (jj_3_1772()) {
    jj_scanpos = xsp;
    if (jj_3_1773()) {
    jj_scanpos = xsp;
    if (jj_3_1774()) {
    jj_scanpos = xsp;
    if (jj_3_1775()) {
    jj_scanpos = xsp;
    if (jj_3_1776()) {
    jj_scanpos = xsp;
    if (jj_3_1777()) {
    jj_scanpos = xsp;
    if (jj_3_1778()) return true;
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    return false;
  }

 inline bool jj_3_183()
 {
    if (jj_done) return true;
    if (jj_scan_token(SERVER_NAME)) return true;
    return false;
  }

 inline bool jj_3_1762()
 {
    if (jj_done) return true;
    if (jj_3R_schema_definition_4298_5_739()) return true;
    return false;
  }

 inline bool jj_3_182()
 {
    if (jj_done) return true;
    if (jj_scan_token(SERIALIZABLE)) return true;
    return false;
  }

 inline bool jj_3_181()
 {
    if (jj_done) return true;
    if (jj_scan_token(SEQUENCE)) return true;
    return false;
  }

 inline bool jj_3_180()
 {
    if (jj_done) return true;
    if (jj_scan_token(SELF)) return true;
    return false;
  }

 inline bool jj_3_179()
 {
    if (jj_done) return true;
    if (jj_scan_token(SECURITY)) return true;
    return false;
  }

 inline bool jj_3_178()
 {
    if (jj_done) return true;
    if (jj_scan_token(SECTION)) return true;
    return false;
  }

 inline bool jj_3_177()
 {
    if (jj_done) return true;
    if (jj_scan_token(SCOPE_SCHEMA)) return true;
    return false;
  }

 inline bool jj_3_1761()
 {
    if (jj_done) return true;
    if (jj_3R_SQL_schema_manipulation_statement_6555_5_738()) return true;
    return false;
  }

 inline bool jj_3R_SQL_schema_statement_6526_5_729()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1760()) {
    jj_scanpos = xsp;
    if (jj_3_1761()) return true;
    }
    return false;
  }

 inline bool jj_3_176()
 {
    if (jj_done) return true;
    if (jj_scan_token(SCOPE_NAME)) return true;
    return false;
  }

 inline bool jj_3_1760()
 {
    if (jj_done) return true;
    if (jj_3R_SQL_schema_definition_statement_6533_5_737()) return true;
    return false;
  }

 inline bool jj_3_175()
 {
    if (jj_done) return true;
    if (jj_scan_token(SCOPE_CATALOG)) return true;
    return false;
  }

 inline bool jj_3_174()
 {
    if (jj_done) return true;
    if (jj_scan_token(SCHEMA_NAME)) return true;
    return false;
  }

 inline bool jj_3_173()
 {
    if (jj_done) return true;
    if (jj_scan_token(SCHEMA)) return true;
    return false;
  }

 inline bool jj_3_172()
 {
    if (jj_done) return true;
    if (jj_scan_token(SCALE)) return true;
    return false;
  }

 inline bool jj_3_171()
 {
    if (jj_done) return true;
    if (jj_scan_token(ROW_COUNT)) return true;
    return false;
  }

 inline bool jj_3_170()
 {
    if (jj_done) return true;
    if (jj_scan_token(ROUTINE_SCHEMA)) return true;
    return false;
  }

 inline bool jj_3_1759()
 {
    if (jj_done) return true;
    if (jj_3R_SQL_dynamic_statement_6657_5_736()) return true;
    return false;
  }

 inline bool jj_3_169()
 {
    if (jj_done) return true;
    if (jj_scan_token(ROUTINE_NAME)) return true;
    return false;
  }

 inline bool jj_3_1758()
 {
    if (jj_done) return true;
    if (jj_3R_SQL_diagnostics_statement_6651_5_735()) return true;
    return false;
  }

 inline bool jj_3_168()
 {
    if (jj_done) return true;
    if (jj_scan_token(ROUTINE_CATALOG)) return true;
    return false;
  }

 inline bool jj_3_1757()
 {
    if (jj_done) return true;
    if (jj_3R_SQL_session_statement_6636_5_734()) return true;
    return false;
  }

 inline bool jj_3_167()
 {
    if (jj_done) return true;
    if (jj_scan_token(ROUTINE)) return true;
    return false;
  }

 inline bool jj_3_1756()
 {
    if (jj_done) return true;
    if (jj_3R_SQL_connection_statement_6628_5_733()) return true;
    return false;
  }

 inline bool jj_3_166()
 {
    if (jj_done) return true;
    if (jj_scan_token(ROLE)) return true;
    return false;
  }

 inline bool jj_3_1755()
 {
    if (jj_done) return true;
    if (jj_3R_SQL_transaction_statement_6616_5_732()) return true;
    return false;
  }

 inline bool jj_3_165()
 {
    if (jj_done) return true;
    if (jj_scan_token(RETURNED_SQLSTATE)) return true;
    return false;
  }

 inline bool jj_3_1754()
 {
    if (jj_done) return true;
    if (jj_3R_SQL_control_statement_6609_5_731()) return true;
    return false;
  }

 inline bool jj_3_164()
 {
    if (jj_done) return true;
    if (jj_scan_token(RETURNED_OCTET_LENGTH)) return true;
    return false;
  }

 inline bool jj_3_1753()
 {
    if (jj_done) return true;
    if (jj_3R_SQL_data_statement_6583_5_730()) return true;
    return false;
  }

 inline bool jj_3R_SQL_executable_statement_6513_5_967()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1752()) {
    jj_scanpos = xsp;
    if (jj_3_1753()) {
    jj_scanpos = xsp;
    if (jj_3_1754()) {
    jj_scanpos = xsp;
    if (jj_3_1755()) {
    jj_scanpos = xsp;
    if (jj_3_1756()) {
    jj_scanpos = xsp;
    if (jj_3_1757()) {
    jj_scanpos = xsp;
    if (jj_3_1758()) {
    jj_scanpos = xsp;
    if (jj_3_1759()) return true;
    }
    }
    }
    }
    }
    }
    }
    return false;
  }

 inline bool jj_3_163()
 {
    if (jj_done) return true;
    if (jj_scan_token(RETURNED_LENGTH)) return true;
    return false;
  }

 inline bool jj_3_1752()
 {
    if (jj_done) return true;
    if (jj_3R_SQL_schema_statement_6526_5_729()) return true;
    return false;
  }

 inline bool jj_3_162()
 {
    if (jj_done) return true;
    if (jj_scan_token(RETURNED_CARDINALITY)) return true;
    return false;
  }

 inline bool jj_3_161()
 {
    if (jj_done) return true;
    if (jj_scan_token(RESTRICT)) return true;
    return false;
  }

 inline bool jj_3_160()
 {
    if (jj_done) return true;
    if (jj_scan_token(RESTART)) return true;
    return false;
  }

 inline bool jj_3_159()
 {
    if (jj_done) return true;
    if (jj_scan_token(RESPECT)) return true;
    return false;
  }

 inline bool jj_3_1751()
 {
    if (jj_done) return true;
    if (jj_3R_locator_indication_5648_5_653()) return true;
    return false;
  }

 inline bool jj_3_158()
 {
    if (jj_done) return true;
    if (jj_scan_token(REPEATABLE)) return true;
    return false;
  }

 inline bool jj_3R_SQL_procedure_statement_6507_5_610()
 {
    if (jj_done) return true;
    if (jj_3R_SQL_executable_statement_6513_5_967()) return true;
    return false;
  }

 inline bool jj_3_157()
 {
    if (jj_done) return true;
    if (jj_scan_token(RELATIVE)) return true;
    return false;
  }

 inline bool jj_3_156()
 {
    if (jj_done) return true;
    if (jj_scan_token(READ)) return true;
    return false;
  }

 inline bool jj_3_155()
 {
    if (jj_done) return true;
    if (jj_scan_token(PUBLIC)) return true;
    return false;
  }

 inline bool jj_3_154()
 {
    if (jj_done) return true;
    if (jj_scan_token(PROPERTIES)) return true;
    return false;
  }

 inline bool jj_3_153()
 {
    if (jj_done) return true;
    if (jj_scan_token(PRIVILEGES)) return true;
    return false;
  }

 inline bool jj_3_152()
 {
    if (jj_done) return true;
    if (jj_scan_token(PRIOR)) return true;
    return false;
  }

 inline bool jj_3_151()
 {
    if (jj_done) return true;
    if (jj_scan_token(PRESERVE)) return true;
    return false;
  }

 inline bool jj_3_150()
 {
    if (jj_done) return true;
    if (jj_scan_token(PRECEDING)) return true;
    return false;
  }

 inline bool jj_3_149()
 {
    if (jj_done) return true;
    if (jj_scan_token(PLI)) return true;
    return false;
  }

 inline bool jj_3_148()
 {
    if (jj_done) return true;
    if (jj_scan_token(PLACING)) return true;
    return false;
  }

 inline bool jj_3_147()
 {
    if (jj_done) return true;
    if (jj_scan_token(PATH)) return true;
    return false;
  }

 inline bool jj_3_146()
 {
    if (jj_done) return true;
    if (jj_scan_token(PASCAL)) return true;
    return false;
  }

 inline bool jj_3R_host_parameter_data_type_6495_5_728()
 {
    if (jj_done) return true;
    if (jj_3R_data_type_1086_3_251()) return true;
    return false;
  }

 inline bool jj_3_145()
 {
    if (jj_done) return true;
    if (jj_scan_token(PARTIAL)) return true;
    return false;
  }

 inline bool jj_3_144()
 {
    if (jj_done) return true;
    if (jj_scan_token(PARAMETER_SPECIFIC_SCHEMA)) return true;
    return false;
  }

 inline bool jj_3_143()
 {
    if (jj_done) return true;
    if (jj_scan_token(PARAMETER_SPECIFIC_NAME)) return true;
    return false;
  }

 inline bool jj_3_142()
 {
    if (jj_done) return true;
    if (jj_scan_token(PARAMETER_SPECIFIC_CATALOG)) return true;
    return false;
  }

 inline bool jj_3_141()
 {
    if (jj_done) return true;
    if (jj_scan_token(PARAMETER_ORDINAL_POSITION)) return true;
    return false;
  }

 inline bool jj_3_140()
 {
    if (jj_done) return true;
    if (jj_scan_token(PARAMETER_NAME)) return true;
    return false;
  }

 inline bool jj_3_1747()
 {
    if (jj_done) return true;
    if (jj_3R_module_character_set_specification_6468_5_725()) return true;
    return false;
  }

 inline bool jj_3_139()
 {
    if (jj_done) return true;
    if (jj_scan_token(PARAMETER_MODE)) return true;
    return false;
  }

 inline bool jj_3_1750()
 {
    if (jj_done) return true;
    if (jj_scan_token(517)) return true;
    return false;
  }

 inline bool jj_3R_host_parameter_declaration_6488_5_726()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1749()) {
    jj_scanpos = xsp;
    if (jj_3_1750()) return true;
    }
    return false;
  }

 inline bool jj_3_138()
 {
    if (jj_done) return true;
    if (jj_scan_token(PAD)) return true;
    return false;
  }

 inline bool jj_3_1749()
 {
    if (jj_done) return true;
    if (jj_3R_host_parameter_name_1007_6_727()) return true;
    if (jj_3R_host_parameter_data_type_6495_5_728()) return true;
    return false;
  }

 inline bool jj_3_137()
 {
    if (jj_done) return true;
    if (jj_scan_token(P)) return true;
    return false;
  }

 inline bool jj_3_136()
 {
    if (jj_done) return true;
    if (jj_scan_token(OVERRIDING)) return true;
    return false;
  }

 inline bool jj_3_135()
 {
    if (jj_done) return true;
    if (jj_scan_token(OUTPUT)) return true;
    return false;
  }

 inline bool jj_3_1748()
 {
    if (jj_done) return true;
    if (jj_scan_token(570)) return true;
    if (jj_3R_host_parameter_declaration_6488_5_726()) return true;
    return false;
  }

 inline bool jj_3_134()
 {
    if (jj_done) return true;
    if (jj_scan_token(OTHERS)) return true;
    return false;
  }

 inline bool jj_3_133()
 {
    if (jj_done) return true;
    if (jj_scan_token(ORDINALITY)) return true;
    return false;
  }

 inline bool jj_3R_host_parameter_declaration_list_6481_6_990()
 {
    if (jj_done) return true;
    if (jj_scan_token(lparen)) return true;
    return false;
  }

 inline bool jj_3_132()
 {
    if (jj_done) return true;
    if (jj_scan_token(ORDERING)) return true;
    return false;
  }

 inline bool jj_3_131()
 {
    if (jj_done) return true;
    if (jj_scan_token(OPTIONS)) return true;
    return false;
  }

 inline bool jj_3_130()
 {
    if (jj_done) return true;
    if (jj_scan_token(OPTION)) return true;
    return false;
  }

 inline bool jj_3_1741()
 {
    if (jj_done) return true;
    if (jj_scan_token(FOR)) return true;
    if (jj_3R_character_set_specification_list_6448_5_721()) return true;
    return false;
  }

 inline bool jj_3_1742()
 {
    if (jj_done) return true;
    if (jj_scan_token(570)) return true;
    if (jj_3R_character_set_specification_4015_5_133()) return true;
    return false;
  }

 inline bool jj_3_129()
 {
    if (jj_done) return true;
    if (jj_scan_token(OCTETS)) return true;
    return false;
  }

 inline bool jj_3_128()
 {
    if (jj_done) return true;
    if (jj_scan_token(OBJECT)) return true;
    return false;
  }

 inline bool jj_3_127()
 {
    if (jj_done) return true;
    if (jj_scan_token(NUMBER)) return true;
    return false;
  }

 inline bool jj_3_126()
 {
    if (jj_done) return true;
    if (jj_scan_token(NULLS)) return true;
    return false;
  }

 inline bool jj_3_125()
 {
    if (jj_done) return true;
    if (jj_scan_token(NULLABLE)) return true;
    return false;
  }

 inline bool jj_3R_externally_invoked_procedure_6474_5_724()
 {
    if (jj_done) return true;
    if (jj_scan_token(PROCEDURE)) return true;
    if (jj_3R_identifier_928_3_141()) return true;
    if (jj_3R_host_parameter_declaration_list_6481_6_990()) return true;
    return false;
  }

 inline bool jj_3_124()
 {
    if (jj_done) return true;
    if (jj_scan_token(NORMALIZED)) return true;
    return false;
  }

 inline bool jj_3_123()
 {
    if (jj_done) return true;
    if (jj_scan_token(NFKD)) return true;
    return false;
  }

 inline bool jj_3_1746()
 {
    if (jj_done) return true;
    if (jj_3R_identifier_928_3_141()) return true;
    return false;
  }

 inline bool jj_3_122()
 {
    if (jj_done) return true;
    if (jj_scan_token(NFKC)) return true;
    return false;
  }

 inline bool jj_3_121()
 {
    if (jj_done) return true;
    if (jj_scan_token(NFD)) return true;
    return false;
  }

 inline bool jj_3_120()
 {
    if (jj_done) return true;
    if (jj_scan_token(NFC)) return true;
    return false;
  }

 inline bool jj_3_119()
 {
    if (jj_done) return true;
    if (jj_scan_token(NEXT)) return true;
    return false;
  }

 inline bool jj_3R_module_character_set_specification_6468_5_725()
 {
    if (jj_done) return true;
    if (jj_scan_token(NAMES)) return true;
    if (jj_scan_token(ARE)) return true;
    if (jj_3R_character_set_specification_4015_5_133()) return true;
    return false;
  }

 inline bool jj_3_118()
 {
    if (jj_done) return true;
    if (jj_scan_token(NESTING)) return true;
    return false;
  }

 inline bool jj_3_117()
 {
    if (jj_done) return true;
    if (jj_scan_token(NAMES)) return true;
    return false;
  }

 inline bool jj_3_116()
 {
    if (jj_done) return true;
    if (jj_scan_token(MUMPS)) return true;
    return false;
  }

 inline bool jj_3_115()
 {
    if (jj_done) return true;
    if (jj_scan_token(MORE_)) return true;
    return false;
  }

 inline bool jj_3_114()
 {
    if (jj_done) return true;
    if (jj_scan_token(MINVALUE)) return true;
    return false;
  }

 inline bool jj_3_113()
 {
    if (jj_done) return true;
    if (jj_scan_token(MESSAGE_TEXT)) return true;
    return false;
  }

 inline bool jj_3_112()
 {
    if (jj_done) return true;
    if (jj_scan_token(MESSAGE_OCTET_LENGTH)) return true;
    return false;
  }

 inline bool jj_3_111()
 {
    if (jj_done) return true;
    if (jj_scan_token(MESSAGE_LENGTH)) return true;
    return false;
  }

 inline bool jj_3_110()
 {
    if (jj_done) return true;
    if (jj_scan_token(MAXVALUE)) return true;
    return false;
  }

 inline bool jj_3_109()
 {
    if (jj_done) return true;
    if (jj_scan_token(MATCHED)) return true;
    return false;
  }

 inline bool jj_3_108()
 {
    if (jj_done) return true;
    if (jj_scan_token(MAP)) return true;
    return false;
  }

 inline bool jj_3_107()
 {
    if (jj_done) return true;
    if (jj_scan_token(M)) return true;
    return false;
  }

 inline bool jj_3_106()
 {
    if (jj_done) return true;
    if (jj_scan_token(LOCATOR)) return true;
    return false;
  }

 inline bool jj_3_1745()
 {
    if (jj_done) return true;
    if (jj_3R_externally_invoked_procedure_6474_5_724()) return true;
    return false;
  }

 inline bool jj_3_105()
 {
    if (jj_done) return true;
    if (jj_scan_token(LEVEL)) return true;
    return false;
  }

 inline bool jj_3_1744()
 {
    if (jj_done) return true;
    if (jj_3R_dynamic_declare_cursor_7675_5_723()) return true;
    return false;
  }

 inline bool jj_3R_module_contents_6454_5_719()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1743()) {
    jj_scanpos = xsp;
    if (jj_3_1744()) {
    jj_scanpos = xsp;
    if (jj_3_1745()) return true;
    }
    }
    return false;
  }

 inline bool jj_3_104()
 {
    if (jj_done) return true;
    if (jj_scan_token(LENGTH)) return true;
    return false;
  }

 inline bool jj_3_1743()
 {
    if (jj_done) return true;
    if (jj_3R_declare_cursor_6689_5_722()) return true;
    return false;
  }

 inline bool jj_3_103()
 {
    if (jj_done) return true;
    if (jj_scan_token(LAST)) return true;
    return false;
  }

 inline bool jj_3_102()
 {
    if (jj_done) return true;
    if (jj_scan_token(KEY_TYPE)) return true;
    return false;
  }

 inline bool jj_3_101()
 {
    if (jj_done) return true;
    if (jj_scan_token(KEY_MEMBER)) return true;
    return false;
  }

 inline bool jj_3_100()
 {
    if (jj_done) return true;
    if (jj_scan_token(KEY)) return true;
    return false;
  }

 inline bool jj_3_99()
 {
    if (jj_done) return true;
    if (jj_scan_token(K)) return true;
    return false;
  }

 inline bool jj_3R_character_set_specification_list_6448_5_721()
 {
    if (jj_done) return true;
    if (jj_3R_character_set_specification_4015_5_133()) return true;
    Token * xsp;
    while (true) {
      xsp = jj_scanpos;
      if (jj_3_1742()) { jj_scanpos = xsp; break; }
    }
    return false;
  }

 inline bool jj_3_98()
 {
    if (jj_done) return true;
    if (jj_scan_token(ISOLATION)) return true;
    return false;
  }

 inline bool jj_3_97()
 {
    if (jj_done) return true;
    if (jj_scan_token(INVOKER)) return true;
    return false;
  }

 inline bool jj_3_96()
 {
    if (jj_done) return true;
    if (jj_scan_token(INSTEAD)) return true;
    return false;
  }

 inline bool jj_3_1735()
 {
    if (jj_done) return true;
    if (jj_scan_token(AND)) return true;
    if (jj_scan_token(DYNAMIC)) return true;
    return false;
  }

 inline bool jj_3_95()
 {
    if (jj_done) return true;
    if (jj_scan_token(INSTANTIABLE)) return true;
    return false;
  }

 inline bool jj_3_1723()
 {
    if (jj_done) return true;
    if (jj_scan_token(570)) return true;
    if (jj_3R_identifier_928_3_141()) return true;
    return false;
  }

 inline bool jj_3_94()
 {
    if (jj_done) return true;
    if (jj_scan_token(INSTANCE)) return true;
    return false;
  }

 inline bool jj_3_1732()
 {
    if (jj_done) return true;
    if (jj_scan_token(AND)) return true;
    if (jj_scan_token(DYNAMIC)) return true;
    return false;
  }

 inline bool jj_3_93()
 {
    if (jj_done) return true;
    if (jj_scan_token(INPUT)) return true;
    return false;
  }

 inline bool jj_3R_module_collation_specification_6442_5_720()
 {
    if (jj_done) return true;
    if (jj_scan_token(COLLATION)) return true;
    if (jj_3R_schema_qualified_name_970_5_252()) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1741()) jj_scanpos = xsp;
    return false;
  }

 inline bool jj_3_92()
 {
    if (jj_done) return true;
    if (jj_scan_token(INITIALLY)) return true;
    return false;
  }

 inline bool jj_3_91()
 {
    if (jj_done) return true;
    if (jj_scan_token(INCREMENT)) return true;
    return false;
  }

 inline bool jj_3_90()
 {
    if (jj_done) return true;
    if (jj_scan_token(INCLUDING)) return true;
    return false;
  }

 inline bool jj_3_89()
 {
    if (jj_done) return true;
    if (jj_scan_token(IMPLEMENTATION)) return true;
    return false;
  }

 inline bool jj_3_88()
 {
    if (jj_done) return true;
    if (jj_scan_token(IMMEDIATE)) return true;
    return false;
  }

 inline bool jj_3_1740()
 {
    if (jj_done) return true;
    if (jj_3R_module_collation_specification_6442_5_720()) return true;
    return false;
  }

 inline bool jj_3_87()
 {
    if (jj_done) return true;
    if (jj_scan_token(IGNORE)) return true;
    return false;
  }

 inline bool jj_3_1734()
 {
    if (jj_done) return true;
    if (jj_scan_token(ONLY)) return true;
    return false;
  }

 inline bool jj_3R_module_collations_6436_5_717()
 {
    if (jj_done) return true;
    Token * xsp;
    if (jj_3_1740()) return true;
    while (true) {
      xsp = jj_scanpos;
      if (jj_3_1740()) { jj_scanpos = xsp; break; }
    }
    return false;
  }

 inline bool jj_3_86()
 {
    if (jj_done) return true;
    if (jj_scan_token(IF)) return true;
    return false;
  }

 inline bool jj_3_85()
 {
    if (jj_done) return true;
    if (jj_scan_token(HIERARCHY)) return true;
    return false;
  }

 inline bool jj_3_1731()
 {
    if (jj_done) return true;
    if (jj_scan_token(ONLY)) return true;
    return false;
  }

 inline bool jj_3_84()
 {
    if (jj_done) return true;
    if (jj_scan_token(GRANTED)) return true;
    return false;
  }

 inline bool jj_3_83()
 {
    if (jj_done) return true;
    if (jj_scan_token(GOTO)) return true;
    return false;
  }

 inline bool jj_3_82()
 {
    if (jj_done) return true;
    if (jj_scan_token(GO)) return true;
    return false;
  }

 inline bool jj_3_81()
 {
    if (jj_done) return true;
    if (jj_scan_token(GENERATED)) return true;
    return false;
  }

 inline bool jj_3R_module_transform_group_specification_6430_5_716()
 {
    if (jj_done) return true;
    if (jj_3R_transform_group_specification_5847_5_669()) return true;
    return false;
  }

 inline bool jj_3_80()
 {
    if (jj_done) return true;
    if (jj_scan_token(GENERAL)) return true;
    return false;
  }

 inline bool jj_3_79()
 {
    if (jj_done) return true;
    if (jj_scan_token(G)) return true;
    return false;
  }

 inline bool jj_3_78()
 {
    if (jj_done) return true;
    if (jj_scan_token(FOUND)) return true;
    return false;
  }

 inline bool jj_3_77()
 {
    if (jj_done) return true;
    if (jj_scan_token(FORTRAN)) return true;
    return false;
  }

 inline bool jj_3_76()
 {
    if (jj_done) return true;
    if (jj_scan_token(FOLLOWING)) return true;
    return false;
  }

 inline bool jj_3_75()
 {
    if (jj_done) return true;
    if (jj_scan_token(FLAG)) return true;
    return false;
  }

 inline bool jj_3R_module_path_specification_6424_5_715()
 {
    if (jj_done) return true;
    if (jj_3R_path_specification_3954_5_954()) return true;
    return false;
  }

 inline bool jj_3_74()
 {
    if (jj_done) return true;
    if (jj_scan_token(FIRST)) return true;
    return false;
  }

 inline bool jj_3_73()
 {
    if (jj_done) return true;
    if (jj_scan_token(FINAL)) return true;
    return false;
  }

 inline bool jj_3_72()
 {
    if (jj_done) return true;
    if (jj_scan_token(EXPRESSION)) return true;
    return false;
  }

 inline bool jj_3_71()
 {
    if (jj_done) return true;
    if (jj_scan_token(EXCLUDING)) return true;
    return false;
  }

 inline bool jj_3_70()
 {
    if (jj_done) return true;
    if (jj_scan_token(EXCLUDE)) return true;
    return false;
  }

 inline bool jj_3_1736()
 {
    if (jj_done) return true;
    if (jj_scan_token(FOR)) return true;
    if (jj_scan_token(STATIC)) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1734()) {
    jj_scanpos = xsp;
    if (jj_3_1735()) return true;
    }
    return false;
  }

 inline bool jj_3_69()
 {
    if (jj_done) return true;
    if (jj_scan_token(EQUALS)) return true;
    return false;
  }

 inline bool jj_3_68()
 {
    if (jj_done) return true;
    if (jj_scan_token(ENFORCED)) return true;
    return false;
  }

 inline bool jj_3_1733()
 {
    if (jj_done) return true;
    if (jj_scan_token(FOR)) return true;
    if (jj_scan_token(STATIC)) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1731()) {
    jj_scanpos = xsp;
    if (jj_3_1732()) return true;
    }
    return false;
  }

 inline bool jj_3_67()
 {
    if (jj_done) return true;
    if (jj_scan_token(DYNAMIC_FUNCTION_CODE)) return true;
    return false;
  }

 inline bool jj_3_1739()
 {
    if (jj_done) return true;
    if (jj_scan_token(SCHEMA)) return true;
    if (jj_3R_schema_name_956_5_140()) return true;
    if (jj_scan_token(AUTHORIZATION)) return true;
    return false;
  }

 inline bool jj_3_66()
 {
    if (jj_done) return true;
    if (jj_scan_token(DYNAMIC_FUNCTION)) return true;
    return false;
  }

 inline bool jj_3_65()
 {
    if (jj_done) return true;
    if (jj_scan_token(DOMAIN)) return true;
    return false;
  }

 inline bool jj_3_1738()
 {
    if (jj_done) return true;
    if (jj_scan_token(AUTHORIZATION)) return true;
    if (jj_3R_identifier_928_3_141()) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1733()) jj_scanpos = xsp;
    return false;
  }

 inline bool jj_3_64()
 {
    if (jj_done) return true;
    if (jj_scan_token(DISPATCH)) return true;
    return false;
  }

 inline bool jj_3_1724()
 {
    if (jj_done) return true;
    if (jj_scan_token(570)) return true;
    if (jj_3R_grantee_6336_5_705()) return true;
    return false;
  }

 inline bool jj_3_1737()
 {
    if (jj_done) return true;
    if (jj_scan_token(SCHEMA)) return true;
    if (jj_3R_schema_name_956_5_140()) return true;
    return false;
  }

 inline bool jj_3_63()
 {
    if (jj_done) return true;
    if (jj_scan_token(DIAGNOSTICS)) return true;
    return false;
  }

 inline bool jj_3_62()
 {
    if (jj_done) return true;
    if (jj_scan_token(DESCRIPTOR)) return true;
    return false;
  }

 inline bool jj_3_61()
 {
    if (jj_done) return true;
    if (jj_scan_token(DESC)) return true;
    return false;
  }

 inline bool jj_3_60()
 {
    if (jj_done) return true;
    if (jj_scan_token(DERIVED)) return true;
    return false;
  }

 inline bool jj_3_1729()
 {
    if (jj_done) return true;
    if (jj_3R_temporary_table_declaration_7036_5_718()) return true;
    return false;
  }

 inline bool jj_3_1730()
 {
    if (jj_done) return true;
    if (jj_3R_module_contents_6454_5_719()) return true;
    return false;
  }

 inline bool jj_3_59()
 {
    if (jj_done) return true;
    if (jj_scan_token(DEPTH)) return true;
    return false;
  }

 inline bool jj_3_58()
 {
    if (jj_done) return true;
    if (jj_scan_token(DEGREE)) return true;
    return false;
  }

 inline bool jj_3_1728()
 {
    if (jj_done) return true;
    if (jj_3R_module_collations_6436_5_717()) return true;
    return false;
  }

 inline bool jj_3_57()
 {
    if (jj_done) return true;
    if (jj_scan_token(DEFINER)) return true;
    return false;
  }

 inline bool jj_3_1727()
 {
    if (jj_done) return true;
    if (jj_3R_module_transform_group_specification_6430_5_716()) return true;
    return false;
  }

 inline bool jj_3_56()
 {
    if (jj_done) return true;
    if (jj_scan_token(DEFINED)) return true;
    return false;
  }

 inline bool jj_3_1726()
 {
    if (jj_done) return true;
    if (jj_3R_module_path_specification_6424_5_715()) return true;
    return false;
  }

 inline bool jj_3_55()
 {
    if (jj_done) return true;
    if (jj_scan_token(DEFERRED)) return true;
    return false;
  }

 inline bool jj_3_1722()
 {
    if (jj_done) return true;
    if (jj_scan_token(ADMIN)) return true;
    if (jj_scan_token(OPTION)) return true;
    if (jj_scan_token(FOR)) return true;
    return false;
  }

 inline bool jj_3_54()
 {
    if (jj_done) return true;
    if (jj_scan_token(DEFERRABLE)) return true;
    return false;
  }

 inline bool jj_3_53()
 {
    if (jj_done) return true;
    if (jj_scan_token(DEFAULTS)) return true;
    return false;
  }

 inline bool jj_3_52()
 {
    if (jj_done) return true;
    if (jj_scan_token(DATETIME_INTERVAL_PRECISION)) return true;
    return false;
  }

 inline bool jj_3_51()
 {
    if (jj_done) return true;
    if (jj_scan_token(DATETIME_INTERVAL_CODE)) return true;
    return false;
  }

 inline bool jj_3_50()
 {
    if (jj_done) return true;
    if (jj_scan_token(DATA)) return true;
    return false;
  }

 inline bool jj_3_49()
 {
    if (jj_done) return true;
    if (jj_scan_token(CURSOR_NAME)) return true;
    return false;
  }

 inline bool jj_3_48()
 {
    if (jj_done) return true;
    if (jj_scan_token(CONTINUE)) return true;
    return false;
  }

 inline bool jj_3_1718()
 {
    if (jj_done) return true;
    if (jj_scan_token(570)) return true;
    if (jj_3R_grantee_6336_5_705()) return true;
    return false;
  }

 inline bool jj_3_1725()
 {
    if (jj_done) return true;
    if (jj_scan_token(GRANTED)) return true;
    if (jj_scan_token(BY)) return true;
    if (jj_3R_grantor_6343_5_706()) return true;
    return false;
  }

 inline bool jj_3_47()
 {
    if (jj_done) return true;
    if (jj_scan_token(CONTAINS)) return true;
    return false;
  }

 inline bool jj_3_46()
 {
    if (jj_done) return true;
    if (jj_scan_token(CONSTRUCTOR)) return true;
    return false;
  }

 inline bool jj_3_45()
 {
    if (jj_done) return true;
    if (jj_scan_token(CONSTRAINTS)) return true;
    return false;
  }

 inline bool jj_3R_revoke_role_statement_6394_5_713()
 {
    if (jj_done) return true;
    if (jj_scan_token(REVOKE)) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1722()) jj_scanpos = xsp;
    if (jj_3R_identifier_928_3_141()) return true;
    while (true) {
      xsp = jj_scanpos;
      if (jj_3_1723()) { jj_scanpos = xsp; break; }
    }
    if (jj_scan_token(FROM)) return true;
    return false;
  }

 inline bool jj_3_44()
 {
    if (jj_done) return true;
    if (jj_scan_token(CONSTRAINT_SCHEMA)) return true;
    return false;
  }

 inline bool jj_3_43()
 {
    if (jj_done) return true;
    if (jj_scan_token(CONSTRAINT_NAME)) return true;
    return false;
  }

 inline bool jj_3_42()
 {
    if (jj_done) return true;
    if (jj_scan_token(CONSTRAINT_CATALOG)) return true;
    return false;
  }

 inline bool jj_3_41()
 {
    if (jj_done) return true;
    if (jj_scan_token(CONNECTION_NAME)) return true;
    return false;
  }

 inline bool jj_3_40()
 {
    if (jj_done) return true;
    if (jj_scan_token(CONNECTION)) return true;
    return false;
  }

 inline bool jj_3_39()
 {
    if (jj_done) return true;
    if (jj_scan_token(CONDITION_NUMBER)) return true;
    return false;
  }

 inline bool jj_3_1717()
 {
    if (jj_done) return true;
    if (jj_3R_revoke_option_extension_6387_5_714()) return true;
    return false;
  }

 inline bool jj_3_38()
 {
    if (jj_done) return true;
    if (jj_scan_token(COMMITTED)) return true;
    return false;
  }

 inline bool jj_3_1721()
 {
    if (jj_done) return true;
    if (jj_scan_token(HIERARCHY)) return true;
    if (jj_scan_token(OPTION)) return true;
    if (jj_scan_token(FOR)) return true;
    return false;
  }

 inline bool jj_3R_revoke_option_extension_6387_5_714()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1720()) {
    jj_scanpos = xsp;
    if (jj_3_1721()) return true;
    }
    return false;
  }

 inline bool jj_3_37()
 {
    if (jj_done) return true;
    if (jj_scan_token(COMMAND_FUNCTION_CODE)) return true;
    return false;
  }

 inline bool jj_3_1720()
 {
    if (jj_done) return true;
    if (jj_scan_token(GRANT)) return true;
    if (jj_scan_token(OPTION)) return true;
    if (jj_scan_token(FOR)) return true;
    return false;
  }

 inline bool jj_3_36()
 {
    if (jj_done) return true;
    if (jj_scan_token(COMMAND_FUNCTION)) return true;
    return false;
  }

 inline bool jj_3_35()
 {
    if (jj_done) return true;
    if (jj_scan_token(COLUMN_NAME)) return true;
    return false;
  }

 inline bool jj_3_34()
 {
    if (jj_done) return true;
    if (jj_scan_token(COLLATION_SCHEMA)) return true;
    return false;
  }

 inline bool jj_3_33()
 {
    if (jj_done) return true;
    if (jj_scan_token(COLLATION_NAME)) return true;
    return false;
  }

 inline bool jj_3_32()
 {
    if (jj_done) return true;
    if (jj_scan_token(COLLATION_CATALOG)) return true;
    return false;
  }

 inline bool jj_3_1719()
 {
    if (jj_done) return true;
    if (jj_scan_token(GRANTED)) return true;
    if (jj_scan_token(BY)) return true;
    if (jj_3R_grantor_6343_5_706()) return true;
    return false;
  }

 inline bool jj_3_31()
 {
    if (jj_done) return true;
    if (jj_scan_token(COLLATION)) return true;
    return false;
  }

 inline bool jj_3_1710()
 {
    if (jj_done) return true;
    if (jj_scan_token(WITH)) return true;
    if (jj_scan_token(ADMIN)) return true;
    if (jj_3R_grantor_6343_5_706()) return true;
    return false;
  }

 inline bool jj_3_30()
 {
    if (jj_done) return true;
    if (jj_scan_token(COBOL)) return true;
    return false;
  }

 inline bool jj_3_29()
 {
    if (jj_done) return true;
    if (jj_scan_token(CLASS_ORIGIN)) return true;
    return false;
  }

 inline bool jj_3_1711()
 {
    if (jj_done) return true;
    if (jj_scan_token(570)) return true;
    if (jj_3R_identifier_928_3_141()) return true;
    return false;
  }

 inline bool jj_3R_revoke_privilege_statement_6378_5_712()
 {
    if (jj_done) return true;
    if (jj_scan_token(REVOKE)) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1717()) jj_scanpos = xsp;
    if (jj_3R_privileges_6282_5_988()) return true;
    return false;
  }

 inline bool jj_3_28()
 {
    if (jj_done) return true;
    if (jj_scan_token(CHARACTERS)) return true;
    return false;
  }

 inline bool jj_3_27()
 {
    if (jj_done) return true;
    if (jj_scan_token(CHARACTERISTICS)) return true;
    return false;
  }

 inline bool jj_3_26()
 {
    if (jj_done) return true;
    if (jj_scan_token(CHARACTER_SET_SCHEMA)) return true;
    return false;
  }

 inline bool jj_3_25()
 {
    if (jj_done) return true;
    if (jj_scan_token(CHARACTER_SET_NAME)) return true;
    return false;
  }

 inline bool jj_3_24()
 {
    if (jj_done) return true;
    if (jj_scan_token(CHARACTER_SET_CATALOG)) return true;
    return false;
  }

 inline bool jj_3_1712()
 {
    if (jj_done) return true;
    if (jj_scan_token(570)) return true;
    if (jj_3R_grantee_6336_5_705()) return true;
    return false;
  }

 inline bool jj_3_23()
 {
    if (jj_done) return true;
    if (jj_scan_token(CHAIN)) return true;
    return false;
  }

 inline bool jj_3_22()
 {
    if (jj_done) return true;
    if (jj_scan_token(CATALOG_NAME)) return true;
    return false;
  }

 inline bool jj_3_1716()
 {
    if (jj_done) return true;
    if (jj_3R_revoke_role_statement_6394_5_713()) return true;
    return false;
  }

 inline bool jj_3R_revoke_statement_6371_5_748()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1715()) {
    jj_scanpos = xsp;
    if (jj_3_1716()) return true;
    }
    return false;
  }

 inline bool jj_3_21()
 {
    if (jj_done) return true;
    if (jj_scan_token(CATALOG)) return true;
    return false;
  }

 inline bool jj_3_1715()
 {
    if (jj_done) return true;
    if (jj_3R_revoke_privilege_statement_6378_5_712()) return true;
    return false;
  }

 inline bool jj_3_20()
 {
    if (jj_done) return true;
    if (jj_scan_token(CASCADE)) return true;
    return false;
  }

 inline bool jj_3_19()
 {
    if (jj_done) return true;
    if (jj_scan_token(C)) return true;
    return false;
  }

 inline bool jj_3_18()
 {
    if (jj_done) return true;
    if (jj_scan_token(BREADTH)) return true;
    return false;
  }

 inline bool jj_3_17()
 {
    if (jj_done) return true;
    if (jj_scan_token(BERNOULLI)) return true;
    return false;
  }

 inline bool jj_3_16()
 {
    if (jj_done) return true;
    if (jj_scan_token(BEFORE)) return true;
    return false;
  }

 inline bool jj_3R_drop_role_statement_6365_5_749()
 {
    if (jj_done) return true;
    if (jj_scan_token(DROP)) return true;
    if (jj_scan_token(ROLE)) return true;
    if (jj_3R_identifier_928_3_141()) return true;
    return false;
  }

 inline bool jj_3_15()
 {
    if (jj_done) return true;
    if (jj_scan_token(ATTRIBUTES)) return true;
    return false;
  }

 inline bool jj_3_14()
 {
    if (jj_done) return true;
    if (jj_scan_token(ATTRIBUTE)) return true;
    return false;
  }

 inline bool jj_3_13()
 {
    if (jj_done) return true;
    if (jj_scan_token(ASSIGNMENT)) return true;
    return false;
  }

 inline bool jj_3_12()
 {
    if (jj_done) return true;
    if (jj_scan_token(ASSERTION)) return true;
    return false;
  }

 inline bool jj_3_11()
 {
    if (jj_done) return true;
    if (jj_scan_token(ASC)) return true;
    return false;
  }

 inline bool jj_3_1714()
 {
    if (jj_done) return true;
    if (jj_scan_token(GRANTED)) return true;
    if (jj_scan_token(BY)) return true;
    if (jj_3R_grantor_6343_5_706()) return true;
    return false;
  }

 inline bool jj_3_10()
 {
    if (jj_done) return true;
    if (jj_scan_token(ALWAYS)) return true;
    return false;
  }

 inline bool jj_3_1713()
 {
    if (jj_done) return true;
    if (jj_scan_token(WITH)) return true;
    if (jj_scan_token(ADMIN)) return true;
    if (jj_scan_token(OPTION)) return true;
    return false;
  }

 inline bool jj_3_9()
 {
    if (jj_done) return true;
    if (jj_scan_token(AFTER)) return true;
    return false;
  }

 inline bool jj_3_8()
 {
    if (jj_done) return true;
    if (jj_scan_token(ADMIN)) return true;
    return false;
  }

 inline bool jj_3_7()
 {
    if (jj_done) return true;
    if (jj_scan_token(ADD)) return true;
    return false;
  }

 inline bool jj_3R_grant_role_statement_6356_5_704()
 {
    if (jj_done) return true;
    if (jj_scan_token(GRANT)) return true;
    if (jj_3R_identifier_928_3_141()) return true;
    Token * xsp;
    while (true) {
      xsp = jj_scanpos;
      if (jj_3_1711()) { jj_scanpos = xsp; break; }
    }
    if (jj_scan_token(TO)) return true;
    return false;
  }

 inline bool jj_3_6()
 {
    if (jj_done) return true;
    if (jj_scan_token(ADA)) return true;
    return false;
  }

 inline bool jj_3_1705()
 {
    if (jj_done) return true;
    if (jj_scan_token(570)) return true;
    if (jj_3R_specific_routine_designator_4041_5_708()) return true;
    return false;
  }

 inline bool jj_3_5()
 {
    if (jj_done) return true;
    if (jj_scan_token(ACTION)) return true;
    return false;
  }

 inline bool jj_3_4()
 {
    if (jj_done) return true;
    if (jj_scan_token(ABSOLUTE)) return true;
    return false;
  }

 inline bool jj_3_3()
 {
    if (jj_done) return true;
    if (jj_scan_token(A)) return true;
    return false;
  }

 inline bool jj_3R_non_reserved_word_210_5_139()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_3()) {
    jj_scanpos = xsp;
    if (jj_3_4()) {
    jj_scanpos = xsp;
    if (jj_3_5()) {
    jj_scanpos = xsp;
    if (jj_3_6()) {
    jj_scanpos = xsp;
    if (jj_3_7()) {
    jj_scanpos = xsp;
    if (jj_3_8()) {
    jj_scanpos = xsp;
    if (jj_3_9()) {
    jj_scanpos = xsp;
    if (jj_3_10()) {
    jj_scanpos = xsp;
    if (jj_3_11()) {
    jj_scanpos = xsp;
    if (jj_3_12()) {
    jj_scanpos = xsp;
    if (jj_3_13()) {
    jj_scanpos = xsp;
    if (jj_3_14()) {
    jj_scanpos = xsp;
    if (jj_3_15()) {
    jj_scanpos = xsp;
    if (jj_3_16()) {
    jj_scanpos = xsp;
    if (jj_3_17()) {
    jj_scanpos = xsp;
    if (jj_3_18()) {
    jj_scanpos = xsp;
    if (jj_3_19()) {
    jj_scanpos = xsp;
    if (jj_3_20()) {
    jj_scanpos = xsp;
    if (jj_3_21()) {
    jj_scanpos = xsp;
    if (jj_3_22()) {
    jj_scanpos = xsp;
    if (jj_3_23()) {
    jj_scanpos = xsp;
    if (jj_3_24()) {
    jj_scanpos = xsp;
    if (jj_3_25()) {
    jj_scanpos = xsp;
    if (jj_3_26()) {
    jj_scanpos = xsp;
    if (jj_3_27()) {
    jj_scanpos = xsp;
    if (jj_3_28()) {
    jj_scanpos = xsp;
    if (jj_3_29()) {
    jj_scanpos = xsp;
    if (jj_3_30()) {
    jj_scanpos = xsp;
    if (jj_3_31()) {
    jj_scanpos = xsp;
    if (jj_3_32()) {
    jj_scanpos = xsp;
    if (jj_3_33()) {
    jj_scanpos = xsp;
    if (jj_3_34()) {
    jj_scanpos = xsp;
    if (jj_3_35()) {
    jj_scanpos = xsp;
    if (jj_3_36()) {
    jj_scanpos = xsp;
    if (jj_3_37()) {
    jj_scanpos = xsp;
    if (jj_3_38()) {
    jj_scanpos = xsp;
    if (jj_3_39()) {
    jj_scanpos = xsp;
    if (jj_3_40()) {
    jj_scanpos = xsp;
    if (jj_3_41()) {
    jj_scanpos = xsp;
    if (jj_3_42()) {
    jj_scanpos = xsp;
    if (jj_3_43()) {
    jj_scanpos = xsp;
    if (jj_3_44()) {
    jj_scanpos = xsp;
    if (jj_3_45()) {
    jj_scanpos = xsp;
    if (jj_3_46()) {
    jj_scanpos = xsp;
    if (jj_3_47()) {
    jj_scanpos = xsp;
    if (jj_3_48()) {
    jj_scanpos = xsp;
    if (jj_3_49()) {
    jj_scanpos = xsp;
    if (jj_3_50()) {
    jj_scanpos = xsp;
    if (jj_3_51()) {
    jj_scanpos = xsp;
    if (jj_3_52()) {
    jj_scanpos = xsp;
    if (jj_3_53()) {
    jj_scanpos = xsp;
    if (jj_3_54()) {
    jj_scanpos = xsp;
    if (jj_3_55()) {
    jj_scanpos = xsp;
    if (jj_3_56()) {
    jj_scanpos = xsp;
    if (jj_3_57()) {
    jj_scanpos = xsp;
    if (jj_3_58()) {
    jj_scanpos = xsp;
    if (jj_3_59()) {
    jj_scanpos = xsp;
    if (jj_3_60()) {
    jj_scanpos = xsp;
    if (jj_3_61()) {
    jj_scanpos = xsp;
    if (jj_3_62()) {
    jj_scanpos = xsp;
    if (jj_3_63()) {
    jj_scanpos = xsp;
    if (jj_3_64()) {
    jj_scanpos = xsp;
    if (jj_3_65()) {
    jj_scanpos = xsp;
    if (jj_3_66()) {
    jj_scanpos = xsp;
    if (jj_3_67()) {
    jj_scanpos = xsp;
    if (jj_3_68()) {
    jj_scanpos = xsp;
    if (jj_3_69()) {
    jj_scanpos = xsp;
    if (jj_3_70()) {
    jj_scanpos = xsp;
    if (jj_3_71()) {
    jj_scanpos = xsp;
    if (jj_3_72()) {
    jj_scanpos = xsp;
    if (jj_3_73()) {
    jj_scanpos = xsp;
    if (jj_3_74()) {
    jj_scanpos = xsp;
    if (jj_3_75()) {
    jj_scanpos = xsp;
    if (jj_3_76()) {
    jj_scanpos = xsp;
    if (jj_3_77()) {
    jj_scanpos = xsp;
    if (jj_3_78()) {
    jj_scanpos = xsp;
    if (jj_3_79()) {
    jj_scanpos = xsp;
    if (jj_3_80()) {
    jj_scanpos = xsp;
    if (jj_3_81()) {
    jj_scanpos = xsp;
    if (jj_3_82()) {
    jj_scanpos = xsp;
    if (jj_3_83()) {
    jj_scanpos = xsp;
    if (jj_3_84()) {
    jj_scanpos = xsp;
    if (jj_3_85()) {
    jj_scanpos = xsp;
    if (jj_3_86()) {
    jj_scanpos = xsp;
    if (jj_3_87()) {
    jj_scanpos = xsp;
    if (jj_3_88()) {
    jj_scanpos = xsp;
    if (jj_3_89()) {
    jj_scanpos = xsp;
    if (jj_3_90()) {
    jj_scanpos = xsp;
    if (jj_3_91()) {
    jj_scanpos = xsp;
    if (jj_3_92()) {
    jj_scanpos = xsp;
    if (jj_3_93()) {
    jj_scanpos = xsp;
    if (jj_3_94()) {
    jj_scanpos = xsp;
    if (jj_3_95()) {
    jj_scanpos = xsp;
    if (jj_3_96()) {
    jj_scanpos = xsp;
    if (jj_3_97()) {
    jj_scanpos = xsp;
    if (jj_3_98()) {
    jj_scanpos = xsp;
    if (jj_3_99()) {
    jj_scanpos = xsp;
    if (jj_3_100()) {
    jj_scanpos = xsp;
    if (jj_3_101()) {
    jj_scanpos = xsp;
    if (jj_3_102()) {
    jj_scanpos = xsp;
    if (jj_3_103()) {
    jj_scanpos = xsp;
    if (jj_3_104()) {
    jj_scanpos = xsp;
    if (jj_3_105()) {
    jj_scanpos = xsp;
    if (jj_3_106()) {
    jj_scanpos = xsp;
    if (jj_3_107()) {
    jj_scanpos = xsp;
    if (jj_3_108()) {
    jj_scanpos = xsp;
    if (jj_3_109()) {
    jj_scanpos = xsp;
    if (jj_3_110()) {
    jj_scanpos = xsp;
    if (jj_3_111()) {
    jj_scanpos = xsp;
    if (jj_3_112()) {
    jj_scanpos = xsp;
    if (jj_3_113()) {
    jj_scanpos = xsp;
    if (jj_3_114()) {
    jj_scanpos = xsp;
    if (jj_3_115()) {
    jj_scanpos = xsp;
    if (jj_3_116()) {
    jj_scanpos = xsp;
    if (jj_3_117()) {
    jj_scanpos = xsp;
    if (jj_3_118()) {
    jj_scanpos = xsp;
    if (jj_3_119()) {
    jj_scanpos = xsp;
    if (jj_3_120()) {
    jj_scanpos = xsp;
    if (jj_3_121()) {
    jj_scanpos = xsp;
    if (jj_3_122()) {
    jj_scanpos = xsp;
    if (jj_3_123()) {
    jj_scanpos = xsp;
    if (jj_3_124()) {
    jj_scanpos = xsp;
    if (jj_3_125()) {
    jj_scanpos = xsp;
    if (jj_3_126()) {
    jj_scanpos = xsp;
    if (jj_3_127()) {
    jj_scanpos = xsp;
    if (jj_3_128()) {
    jj_scanpos = xsp;
    if (jj_3_129()) {
    jj_scanpos = xsp;
    if (jj_3_130()) {
    jj_scanpos = xsp;
    if (jj_3_131()) {
    jj_scanpos = xsp;
    if (jj_3_132()) {
    jj_scanpos = xsp;
    if (jj_3_133()) {
    jj_scanpos = xsp;
    if (jj_3_134()) {
    jj_scanpos = xsp;
    if (jj_3_135()) {
    jj_scanpos = xsp;
    if (jj_3_136()) {
    jj_scanpos = xsp;
    if (jj_3_137()) {
    jj_scanpos = xsp;
    if (jj_3_138()) {
    jj_scanpos = xsp;
    if (jj_3_139()) {
    jj_scanpos = xsp;
    if (jj_3_140()) {
    jj_scanpos = xsp;
    if (jj_3_141()) {
    jj_scanpos = xsp;
    if (jj_3_142()) {
    jj_scanpos = xsp;
    if (jj_3_143()) {
    jj_scanpos = xsp;
    if (jj_3_144()) {
    jj_scanpos = xsp;
    if (jj_3_145()) {
    jj_scanpos = xsp;
    if (jj_3_146()) {
    jj_scanpos = xsp;
    if (jj_3_147()) {
    jj_scanpos = xsp;
    if (jj_3_148()) {
    jj_scanpos = xsp;
    if (jj_3_149()) {
    jj_scanpos = xsp;
    if (jj_3_150()) {
    jj_scanpos = xsp;
    if (jj_3_151()) {
    jj_scanpos = xsp;
    if (jj_3_152()) {
    jj_scanpos = xsp;
    if (jj_3_153()) {
    jj_scanpos = xsp;
    if (jj_3_154()) {
    jj_scanpos = xsp;
    if (jj_3_155()) {
    jj_scanpos = xsp;
    if (jj_3_156()) {
    jj_scanpos = xsp;
    if (jj_3_157()) {
    jj_scanpos = xsp;
    if (jj_3_158()) {
    jj_scanpos = xsp;
    if (jj_3_159()) {
    jj_scanpos = xsp;
    if (jj_3_160()) {
    jj_scanpos = xsp;
    if (jj_3_161()) {
    jj_scanpos = xsp;
    if (jj_3_162()) {
    jj_scanpos = xsp;
    if (jj_3_163()) {
    jj_scanpos = xsp;
    if (jj_3_164()) {
    jj_scanpos = xsp;
    if (jj_3_165()) {
    jj_scanpos = xsp;
    if (jj_3_166()) {
    jj_scanpos = xsp;
    if (jj_3_167()) {
    jj_scanpos = xsp;
    if (jj_3_168()) {
    jj_scanpos = xsp;
    if (jj_3_169()) {
    jj_scanpos = xsp;
    if (jj_3_170()) {
    jj_scanpos = xsp;
    if (jj_3_171()) {
    jj_scanpos = xsp;
    if (jj_3_172()) {
    jj_scanpos = xsp;
    if (jj_3_173()) {
    jj_scanpos = xsp;
    if (jj_3_174()) {
    jj_scanpos = xsp;
    if (jj_3_175()) {
    jj_scanpos = xsp;
    if (jj_3_176()) {
    jj_scanpos = xsp;
    if (jj_3_177()) {
    jj_scanpos = xsp;
    if (jj_3_178()) {
    jj_scanpos = xsp;
    if (jj_3_179()) {
    jj_scanpos = xsp;
    if (jj_3_180()) {
    jj_scanpos = xsp;
    if (jj_3_181()) {
    jj_scanpos = xsp;
    if (jj_3_182()) {
    jj_scanpos = xsp;
    if (jj_3_183()) {
    jj_scanpos = xsp;
    if (jj_3_184()) {
    jj_scanpos = xsp;
    if (jj_3_185()) {
    jj_scanpos = xsp;
    if (jj_3_186()) {
    jj_scanpos = xsp;
    if (jj_3_187()) {
    jj_scanpos = xsp;
    if (jj_3_188()) {
    jj_scanpos = xsp;
    if (jj_3_189()) {
    jj_scanpos = xsp;
    if (jj_3_190()) {
    jj_scanpos = xsp;
    if (jj_3_191()) {
    jj_scanpos = xsp;
    if (jj_3_192()) {
    jj_scanpos = xsp;
    if (jj_3_193()) {
    jj_scanpos = xsp;
    if (jj_3_194()) {
    jj_scanpos = xsp;
    if (jj_3_195()) {
    jj_scanpos = xsp;
    if (jj_3_196()) {
    jj_scanpos = xsp;
    if (jj_3_197()) {
    jj_scanpos = xsp;
    if (jj_3_198()) {
    jj_scanpos = xsp;
    if (jj_3_199()) {
    jj_scanpos = xsp;
    if (jj_3_200()) {
    jj_scanpos = xsp;
    if (jj_3_201()) {
    jj_scanpos = xsp;
    if (jj_3_202()) {
    jj_scanpos = xsp;
    if (jj_3_203()) {
    jj_scanpos = xsp;
    if (jj_3_204()) {
    jj_scanpos = xsp;
    if (jj_3_205()) {
    jj_scanpos = xsp;
    if (jj_3_206()) {
    jj_scanpos = xsp;
    if (jj_3_207()) {
    jj_scanpos = xsp;
    if (jj_3_208()) {
    jj_scanpos = xsp;
    if (jj_3_209()) {
    jj_scanpos = xsp;
    if (jj_3_210()) {
    jj_scanpos = xsp;
    if (jj_3_211()) {
    jj_scanpos = xsp;
    if (jj_3_212()) {
    jj_scanpos = xsp;
    if (jj_3_213()) {
    jj_scanpos = xsp;
    if (jj_3_214()) {
    jj_scanpos = xsp;
    if (jj_3_215()) {
    jj_scanpos = xsp;
    if (jj_3_216()) {
    jj_scanpos = xsp;
    if (jj_3_217()) {
    jj_scanpos = xsp;
    if (jj_3_218()) {
    jj_scanpos = xsp;
    if (jj_3_219()) {
    jj_scanpos = xsp;
    if (jj_3_220()) {
    jj_scanpos = xsp;
    if (jj_3_221()) {
    jj_scanpos = xsp;
    if (jj_3_222()) {
    jj_scanpos = xsp;
    if (jj_3_223()) {
    jj_scanpos = xsp;
    if (jj_3_224()) {
    jj_scanpos = xsp;
    if (jj_3_225()) {
    jj_scanpos = xsp;
    if (jj_3_226()) {
    jj_scanpos = xsp;
    if (jj_3_227()) {
    jj_scanpos = xsp;
    if (jj_3_228()) {
    jj_scanpos = xsp;
    if (jj_3_229()) {
    jj_scanpos = xsp;
    if (jj_3_230()) {
    jj_scanpos = xsp;
    if (jj_3_231()) {
    jj_scanpos = xsp;
    if (jj_3_232()) {
    jj_scanpos = xsp;
    if (jj_3_233()) {
    jj_scanpos = xsp;
    if (jj_3_234()) {
    jj_scanpos = xsp;
    if (jj_3_235()) {
    jj_scanpos = xsp;
    if (jj_3_236()) {
    jj_scanpos = xsp;
    if (jj_3_237()) {
    jj_scanpos = xsp;
    if (jj_3_238()) {
    jj_scanpos = xsp;
    if (jj_3_239()) {
    jj_scanpos = xsp;
    if (jj_3_240()) {
    jj_scanpos = xsp;
    if (jj_3_241()) {
    jj_scanpos = xsp;
    if (jj_3_242()) {
    jj_scanpos = xsp;
    if (jj_3_243()) {
    jj_scanpos = xsp;
    if (jj_3_244()) {
    jj_scanpos = xsp;
    if (jj_3_245()) {
    jj_scanpos = xsp;
    if (jj_3_246()) {
    jj_scanpos = xsp;
    if (jj_3_247()) {
    jj_scanpos = xsp;
    if (jj_3_248()) {
    jj_scanpos = xsp;
    if (jj_3_249()) {
    jj_scanpos = xsp;
    if (jj_3_250()) {
    jj_scanpos = xsp;
    if (jj_3_251()) {
    jj_scanpos = xsp;
    if (jj_3_252()) {
    jj_scanpos = xsp;
    if (jj_3_253()) {
    jj_scanpos = xsp;
    if (jj_3_254()) {
    jj_scanpos = xsp;
    if (jj_3_255()) {
    jj_scanpos = xsp;
    if (jj_3_256()) {
    jj_scanpos = xsp;
    if (jj_3_257()) {
    jj_scanpos = xsp;
    if (jj_3_258()) {
    jj_scanpos = xsp;
    if (jj_3_259()) {
    jj_scanpos = xsp;
    if (jj_3_260()) {
    jj_scanpos = xsp;
    if (jj_3_261()) {
    jj_scanpos = xsp;
    if (jj_3_262()) {
    jj_scanpos = xsp;
    if (jj_3_263()) {
    jj_scanpos = xsp;
    if (jj_3_264()) {
    jj_scanpos = xsp;
    if (jj_3_265()) {
    jj_scanpos = xsp;
    if (jj_3_266()) {
    jj_scanpos = xsp;
    if (jj_3_267()) {
    jj_scanpos = xsp;
    if (jj_3_268()) {
    jj_scanpos = xsp;
    if (jj_3_269()) {
    jj_scanpos = xsp;
    if (jj_3_270()) {
    jj_scanpos = xsp;
    if (jj_3_271()) {
    jj_scanpos = xsp;
    if (jj_3_272()) {
    jj_scanpos = xsp;
    if (jj_3_273()) {
    jj_scanpos = xsp;
    if (jj_3_274()) {
    jj_scanpos = xsp;
    if (jj_3_275()) {
    jj_scanpos = xsp;
    if (jj_3_276()) {
    jj_scanpos = xsp;
    if (jj_3_277()) {
    jj_scanpos = xsp;
    if (jj_3_278()) {
    jj_scanpos = xsp;
    if (jj_3_279()) {
    jj_scanpos = xsp;
    if (jj_3_280()) {
    jj_scanpos = xsp;
    if (jj_3_281()) {
    jj_scanpos = xsp;
    if (jj_3_282()) {
    jj_scanpos = xsp;
    if (jj_3_283()) {
    jj_scanpos = xsp;
    if (jj_3_284()) {
    jj_scanpos = xsp;
    if (jj_3_285()) {
    jj_scanpos = xsp;
    if (jj_3_286()) {
    jj_scanpos = xsp;
    if (jj_3_287()) {
    jj_scanpos = xsp;
    if (jj_3_288()) {
    jj_scanpos = xsp;
    if (jj_3_289()) {
    jj_scanpos = xsp;
    if (jj_3_290()) {
    jj_scanpos = xsp;
    if (jj_3_291()) {
    jj_scanpos = xsp;
    if (jj_3_292()) {
    jj_scanpos = xsp;
    if (jj_3_293()) {
    jj_scanpos = xsp;
    if (jj_3_294()) {
    jj_scanpos = xsp;
    if (jj_3_295()) {
    jj_scanpos = xsp;
    if (jj_3_296()) {
    jj_scanpos = xsp;
    if (jj_3_297()) {
    jj_scanpos = xsp;
    if (jj_3_298()) {
    jj_scanpos = xsp;
    if (jj_3_299()) {
    jj_scanpos = xsp;
    if (jj_3_300()) {
    jj_scanpos = xsp;
    if (jj_3_301()) {
    jj_scanpos = xsp;
    if (jj_3_302()) {
    jj_scanpos = xsp;
    if (jj_3_303()) {
    jj_scanpos = xsp;
    if (jj_3_304()) {
    jj_scanpos = xsp;
    if (jj_3_305()) {
    jj_scanpos = xsp;
    if (jj_3_306()) {
    jj_scanpos = xsp;
    if (jj_3_307()) {
    jj_scanpos = xsp;
    if (jj_3_308()) {
    jj_scanpos = xsp;
    if (jj_3_309()) {
    jj_scanpos = xsp;
    if (jj_3_310()) {
    jj_scanpos = xsp;
    if (jj_3_311()) {
    jj_scanpos = xsp;
    if (jj_3_312()) {
    jj_scanpos = xsp;
    if (jj_3_313()) {
    jj_scanpos = xsp;
    if (jj_3_314()) {
    jj_scanpos = xsp;
    if (jj_3_315()) {
    jj_scanpos = xsp;
    if (jj_3_316()) {
    jj_scanpos = xsp;
    if (jj_3_317()) {
    jj_scanpos = xsp;
    if (jj_3_318()) {
    jj_scanpos = xsp;
    if (jj_3_319()) {
    jj_scanpos = xsp;
    if (jj_3_320()) {
    jj_scanpos = xsp;
    if (jj_3_321()) {
    jj_scanpos = xsp;
    if (jj_3_322()) {
    jj_scanpos = xsp;
    if (jj_3_323()) {
    jj_scanpos = xsp;
    if (jj_3_324()) {
    jj_scanpos = xsp;
    if (jj_3_325()) {
    jj_scanpos = xsp;
    if (jj_3_326()) {
    jj_scanpos = xsp;
    if (jj_3_327()) {
    jj_scanpos = xsp;
    if (jj_3_328()) {
    jj_scanpos = xsp;
    if (jj_3_329()) {
    jj_scanpos = xsp;
    if (jj_3_330()) {
    jj_scanpos = xsp;
    if (jj_3_331()) {
    jj_scanpos = xsp;
    if (jj_3_332()) {
    jj_scanpos = xsp;
    if (jj_3_333()) {
    jj_scanpos = xsp;
    if (jj_3_334()) {
    jj_scanpos = xsp;
    if (jj_3_335()) {
    jj_scanpos = xsp;
    if (jj_3_336()) {
    jj_scanpos = xsp;
    if (jj_3_337()) {
    jj_scanpos = xsp;
    if (jj_3_338()) {
    jj_scanpos = xsp;
    if (jj_3_339()) {
    jj_scanpos = xsp;
    if (jj_3_340()) {
    jj_scanpos = xsp;
    if (jj_3_341()) {
    jj_scanpos = xsp;
    if (jj_3_342()) {
    jj_scanpos = xsp;
    if (jj_3_343()) {
    jj_scanpos = xsp;
    if (jj_3_344()) {
    jj_scanpos = xsp;
    if (jj_3_345()) {
    jj_scanpos = xsp;
    if (jj_3_346()) return true;
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    return false;
  }

 inline bool jj_3R_role_definition_6350_5_519()
 {
    if (jj_done) return true;
    if (jj_scan_token(CREATE)) return true;
    if (jj_scan_token(ROLE)) return true;
    if (jj_3R_identifier_928_3_141()) return true;
    return false;
  }

 inline bool jj_3_1709()
 {
    if (jj_done) return true;
    if (jj_scan_token(CURRENT_ROLE)) return true;
    return false;
  }

 inline bool jj_3R_grantor_6343_5_706()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1708()) {
    jj_scanpos = xsp;
    if (jj_3_1709()) return true;
    }
    return false;
  }

 inline bool jj_3_1708()
 {
    if (jj_done) return true;
    if (jj_scan_token(CURRENT_USER)) return true;
    return false;
  }

 inline bool jj_3_2()
 {
    if (jj_done) return true;
    if (jj_scan_token(semicolon)) return true;
    return false;
  }

 inline bool jj_3_1707()
 {
    if (jj_done) return true;
    if (jj_3R_identifier_928_3_141()) return true;
    return false;
  }

 inline bool jj_3R_grantee_6336_5_705()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1706()) {
    jj_scanpos = xsp;
    if (jj_3_1707()) return true;
    }
    return false;
  }

 inline bool jj_3_1706()
 {
    if (jj_done) return true;
    if (jj_scan_token(PUBLIC)) return true;
    return false;
  }

 inline bool jj_3R_privilege_column_list_6330_5_710()
 {
    if (jj_done) return true;
    if (jj_3R_column_name_list_2942_5_191()) return true;
    return false;
  }

 inline bool jj_3_1693()
 {
    if (jj_done) return true;
    if (jj_scan_token(lparen)) return true;
    if (jj_3R_privilege_column_list_6330_5_710()) return true;
    if (jj_scan_token(rparen)) return true;
    return false;
  }

 inline bool jj_3R_privilege_method_list_6324_5_711()
 {
    if (jj_done) return true;
    if (jj_3R_specific_routine_designator_4041_5_708()) return true;
    return false;
  }

 inline bool jj_3_1692()
 {
    if (jj_done) return true;
    if (jj_scan_token(lparen)) return true;
    if (jj_3R_privilege_column_list_6330_5_710()) return true;
    if (jj_scan_token(rparen)) return true;
    return false;
  }

 inline bool jj_3_1691()
 {
    if (jj_done) return true;
    if (jj_scan_token(lparen)) return true;
    if (jj_3R_privilege_column_list_6330_5_710()) return true;
    if (jj_scan_token(rparen)) return true;
    return false;
  }

 inline bool jj_3_1()
 {
    if (jj_done) return true;
    if (jj_scan_token(semicolon)) return true;
    return false;
  }

 inline bool jj_3_1704()
 {
    if (jj_done) return true;
    if (jj_scan_token(EXECUTE)) return true;
    return false;
  }

 inline bool jj_3_1703()
 {
    if (jj_done) return true;
    if (jj_scan_token(UNDER)) return true;
    return false;
  }

 inline bool jj_3_1702()
 {
    if (jj_done) return true;
    if (jj_scan_token(TRIGGER)) return true;
    return false;
  }

 inline bool jj_3_1701()
 {
    if (jj_done) return true;
    if (jj_scan_token(USAGE)) return true;
    return false;
  }

 inline bool jj_3_1700()
 {
    if (jj_done) return true;
    if (jj_scan_token(REFERENCES)) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1693()) jj_scanpos = xsp;
    return false;
  }

 inline bool jj_3_1688()
 {
    if (jj_done) return true;
    if (jj_scan_token(570)) return true;
    if (jj_3R_action_6308_5_709()) return true;
    return false;
  }

 inline bool jj_3_1699()
 {
    if (jj_done) return true;
    if (jj_scan_token(UPDATE)) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1692()) jj_scanpos = xsp;
    return false;
  }

 inline bool jj_3_1698()
 {
    if (jj_done) return true;
    if (jj_scan_token(INSERT)) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1691()) jj_scanpos = xsp;
    return false;
  }

 inline bool jj_3_1675()
 {
    if (jj_done) return true;
    if (jj_scan_token(570)) return true;
    if (jj_3R_grantee_6336_5_705()) return true;
    return false;
  }

 inline bool jj_3_1697()
 {
    if (jj_done) return true;
    if (jj_scan_token(DELETE)) return true;
    return false;
  }

 inline bool jj_3_1696()
 {
    if (jj_done) return true;
    if (jj_scan_token(SELECT)) return true;
    if (jj_scan_token(lparen)) return true;
    if (jj_3R_privilege_method_list_6324_5_711()) return true;
    return false;
  }

 inline bool jj_3_1695()
 {
    if (jj_done) return true;
    if (jj_scan_token(SELECT)) return true;
    if (jj_scan_token(lparen)) return true;
    if (jj_3R_privilege_column_list_6330_5_710()) return true;
    return false;
  }

 inline bool jj_3R_action_6308_5_709()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1694()) {
    jj_scanpos = xsp;
    if (jj_3_1695()) {
    jj_scanpos = xsp;
    if (jj_3_1696()) {
    jj_scanpos = xsp;
    if (jj_3_1697()) {
    jj_scanpos = xsp;
    if (jj_3_1698()) {
    jj_scanpos = xsp;
    if (jj_3_1699()) {
    jj_scanpos = xsp;
    if (jj_3_1700()) {
    jj_scanpos = xsp;
    if (jj_3_1701()) {
    jj_scanpos = xsp;
    if (jj_3_1702()) {
    jj_scanpos = xsp;
    if (jj_3_1703()) {
    jj_scanpos = xsp;
    if (jj_3_1704()) return true;
    }
    }
    }
    }
    }
    }
    }
    }
    }
    }
    return false;
  }

 inline bool jj_3_1694()
 {
    if (jj_done) return true;
    if (jj_scan_token(SELECT)) return true;
    return false;
  }

 inline bool jj_3_1690()
 {
    if (jj_done) return true;
    if (jj_3R_action_6308_5_709()) return true;
    Token * xsp;
    while (true) {
      xsp = jj_scanpos;
      if (jj_3_1688()) { jj_scanpos = xsp; break; }
    }
    return false;
  }

 inline bool jj_3R_object_privileges_6301_5_1033()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1689()) {
    jj_scanpos = xsp;
    if (jj_3_1690()) return true;
    }
    return false;
  }

 inline bool jj_3_1689()
 {
    if (jj_done) return true;
    if (jj_scan_token(ALL)) return true;
    if (jj_scan_token(PRIVILEGES)) return true;
    return false;
  }

 inline bool jj_3_1687()
 {
    if (jj_done) return true;
    if (jj_3R_specific_routine_designator_4041_5_708()) return true;
    return false;
  }

 inline bool jj_3_1686()
 {
    if (jj_done) return true;
    if (jj_scan_token(SEQUENCE)) return true;
    if (jj_3R_schema_qualified_name_970_5_252()) return true;
    return false;
  }

 inline bool jj_3_1685()
 {
    if (jj_done) return true;
    if (jj_scan_token(TYPE)) return true;
    if (jj_3R_schema_resolved_user_defined_type_name_1026_5_479()) return true;
    return false;
  }

 inline bool jj_3_1684()
 {
    if (jj_done) return true;
    if (jj_scan_token(TRANSLATION)) return true;
    if (jj_3R_schema_qualified_name_970_5_252()) return true;
    return false;
  }

 inline bool jj_3_1683()
 {
    if (jj_done) return true;
    if (jj_scan_token(CHARACTER)) return true;
    if (jj_scan_token(SET)) return true;
    if (jj_3R_character_set_name_1020_5_707()) return true;
    return false;
  }

 inline bool jj_3_1679()
 {
    if (jj_done) return true;
    if (jj_scan_token(TABLE)) return true;
    return false;
  }

 inline bool jj_3_1682()
 {
    if (jj_done) return true;
    if (jj_scan_token(COLLATION)) return true;
    if (jj_3R_schema_qualified_name_970_5_252()) return true;
    return false;
  }

 inline bool jj_3_1681()
 {
    if (jj_done) return true;
    if (jj_scan_token(DOMAIN)) return true;
    if (jj_3R_schema_qualified_name_970_5_252()) return true;
    return false;
  }

 inline bool jj_3_1680()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1679()) jj_scanpos = xsp;
    if (jj_3R_table_name_948_5_382()) return true;
    return false;
  }

 inline bool jj_3R_privileges_6282_5_988()
 {
    if (jj_done) return true;
    if (jj_3R_object_privileges_6301_5_1033()) return true;
    if (jj_scan_token(ON)) return true;
    return false;
  }

 inline bool jj_3_1678()
 {
    if (jj_done) return true;
    if (jj_scan_token(GRANTED)) return true;
    if (jj_scan_token(BY)) return true;
    if (jj_3R_grantor_6343_5_706()) return true;
    return false;
  }

 inline bool jj_3_1677()
 {
    if (jj_done) return true;
    if (jj_scan_token(WITH)) return true;
    if (jj_scan_token(GRANT)) return true;
    if (jj_scan_token(OPTION)) return true;
    return false;
  }

 inline bool jj_3_1676()
 {
    if (jj_done) return true;
    if (jj_scan_token(WITH)) return true;
    if (jj_scan_token(HIERARCHY)) return true;
    if (jj_scan_token(OPTION)) return true;
    return false;
  }

 inline bool jj_3R_grant_privilege_statement_6273_5_703()
 {
    if (jj_done) return true;
    if (jj_scan_token(GRANT)) return true;
    if (jj_3R_privileges_6282_5_988()) return true;
    return false;
  }

 inline bool jj_3_1674()
 {
    if (jj_done) return true;
    if (jj_3R_grant_role_statement_6356_5_704()) return true;
    return false;
  }

 inline bool jj_3R_grant_statement_6266_5_518()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1673()) {
    jj_scanpos = xsp;
    if (jj_3_1674()) return true;
    }
    return false;
  }

 inline bool jj_3_1673()
 {
    if (jj_done) return true;
    if (jj_3R_grant_privilege_statement_6273_5_703()) return true;
    return false;
  }

 inline bool jj_3R_drop_sequence_generator_statement_6260_5_763()
 {
    if (jj_done) return true;
    if (jj_scan_token(DROP)) return true;
    if (jj_scan_token(SEQUENCE)) return true;
    if (jj_3R_schema_qualified_name_970_5_252()) return true;
    return false;
  }

 inline bool jj_3_1672()
 {
    if (jj_done) return true;
    if (jj_scan_token(WITH)) return true;
    if (jj_3R_sequence_generator_restart_value_6254_5_702()) return true;
    return false;
  }

 inline bool jj_3R_sequence_generator_restart_value_6254_5_702()
 {
    if (jj_done) return true;
    if (jj_3R_signed_numeric_literal_825_5_124()) return true;
    return false;
  }

 inline bool jj_3R_alter_sequence_generator_restart_option_6248_5_590()
 {
    if (jj_done) return true;
    if (jj_scan_token(RESTART)) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1672()) jj_scanpos = xsp;
    return false;
  }

 inline bool jj_3_1671()
 {
    if (jj_done) return true;
    if (jj_3R_basic_sequence_generator_option_6157_5_591()) return true;
    return false;
  }

 inline bool jj_3R_alter_sequence_generator_option_6241_5_701()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1670()) {
    jj_scanpos = xsp;
    if (jj_3_1671()) return true;
    }
    return false;
  }

 inline bool jj_3_1670()
 {
    if (jj_done) return true;
    if (jj_3R_alter_sequence_generator_restart_option_6248_5_590()) return true;
    return false;
  }

 inline bool jj_3_1669()
 {
    if (jj_done) return true;
    if (jj_3R_alter_sequence_generator_option_6241_5_701()) return true;
    return false;
  }

 inline bool jj_3R_alter_sequence_generator_statement_6229_5_762()
 {
    if (jj_done) return true;
    if (jj_scan_token(ALTER)) return true;
    if (jj_scan_token(SEQUENCE)) return true;
    if (jj_3R_schema_qualified_name_970_5_252()) return true;
    return false;
  }

 inline bool jj_3_1668()
 {
    if (jj_done) return true;
    if (jj_scan_token(NO)) return true;
    if (jj_scan_token(CYCLE)) return true;
    return false;
  }

 inline bool jj_3R_sequence_generator_cycle_option_6222_5_698()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1667()) {
    jj_scanpos = xsp;
    if (jj_3_1668()) return true;
    }
    return false;
  }

 inline bool jj_3_1667()
 {
    if (jj_done) return true;
    if (jj_scan_token(CYCLE)) return true;
    return false;
  }

 inline bool jj_3R_sequence_generator_min_value_6216_5_700()
 {
    if (jj_done) return true;
    if (jj_3R_signed_numeric_literal_825_5_124()) return true;
    return false;
  }

 inline bool jj_3_1666()
 {
    if (jj_done) return true;
    if (jj_scan_token(NO)) return true;
    if (jj_scan_token(MINVALUE)) return true;
    return false;
  }

 inline bool jj_3R_sequence_generator_minvalue_option_6209_5_697()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1665()) {
    jj_scanpos = xsp;
    if (jj_3_1666()) return true;
    }
    return false;
  }

 inline bool jj_3_1665()
 {
    if (jj_done) return true;
    if (jj_scan_token(MINVALUE)) return true;
    if (jj_3R_sequence_generator_min_value_6216_5_700()) return true;
    return false;
  }

 inline bool jj_3R_sequence_generator_max_value_6203_5_699()
 {
    if (jj_done) return true;
    if (jj_3R_signed_numeric_literal_825_5_124()) return true;
    return false;
  }

 inline bool jj_3_1664()
 {
    if (jj_done) return true;
    if (jj_scan_token(NO)) return true;
    if (jj_scan_token(MAXVALUE)) return true;
    return false;
  }

 inline bool jj_3R_sequence_generator_maxvalue_option_6196_5_696()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1663()) {
    jj_scanpos = xsp;
    if (jj_3_1664()) return true;
    }
    return false;
  }

 inline bool jj_3_1663()
 {
    if (jj_done) return true;
    if (jj_scan_token(MAXVALUE)) return true;
    if (jj_3R_sequence_generator_max_value_6203_5_699()) return true;
    return false;
  }

 inline bool jj_3R_sequence_generator_increment_6190_5_987()
 {
    if (jj_done) return true;
    if (jj_3R_signed_numeric_literal_825_5_124()) return true;
    return false;
  }

 inline bool jj_3R_sequence_generator_increment_by_option_6184_5_695()
 {
    if (jj_done) return true;
    if (jj_scan_token(INCREMENT)) return true;
    if (jj_scan_token(BY)) return true;
    if (jj_3R_sequence_generator_increment_6190_5_987()) return true;
    return false;
  }

 inline bool jj_3R_sequence_generator_start_value_6178_5_986()
 {
    if (jj_done) return true;
    if (jj_3R_signed_numeric_literal_825_5_124()) return true;
    return false;
  }

 inline bool jj_3R_sequence_generator_start_with_option_6172_5_694()
 {
    if (jj_done) return true;
    if (jj_scan_token(START)) return true;
    if (jj_scan_token(WITH)) return true;
    if (jj_3R_sequence_generator_start_value_6178_5_986()) return true;
    return false;
  }

 inline bool jj_3_1652()
 {
    if (jj_done) return true;
    if (jj_3R_sequence_generator_options_6131_5_690()) return true;
    return false;
  }

 inline bool jj_3R_sequence_generator_data_type_option_6166_5_692()
 {
    if (jj_done) return true;
    if (jj_scan_token(AS)) return true;
    if (jj_3R_data_type_1086_3_251()) return true;
    return false;
  }

 inline bool jj_3_1662()
 {
    if (jj_done) return true;
    if (jj_3R_sequence_generator_cycle_option_6222_5_698()) return true;
    return false;
  }

 inline bool jj_3_1661()
 {
    if (jj_done) return true;
    if (jj_3R_sequence_generator_minvalue_option_6209_5_697()) return true;
    return false;
  }

 inline bool jj_3_1660()
 {
    if (jj_done) return true;
    if (jj_3R_sequence_generator_maxvalue_option_6196_5_696()) return true;
    return false;
  }

 inline bool jj_3R_basic_sequence_generator_option_6157_5_591()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1659()) {
    jj_scanpos = xsp;
    if (jj_3_1660()) {
    jj_scanpos = xsp;
    if (jj_3_1661()) {
    jj_scanpos = xsp;
    if (jj_3_1662()) return true;
    }
    }
    }
    return false;
  }

 inline bool jj_3_1659()
 {
    if (jj_done) return true;
    if (jj_3R_sequence_generator_increment_by_option_6184_5_695()) return true;
    return false;
  }

 inline bool jj_3_1658()
 {
    if (jj_done) return true;
    if (jj_3R_basic_sequence_generator_option_6157_5_591()) return true;
    return false;
  }

 inline bool jj_3R_common_sequence_generator_option_6150_5_693()
 {
    if (jj_done) return true;
    Token * xsp;
    xsp = jj_scanpos;
    if (jj_3_1657()) {
    jj_scanpos = xsp;
    if (jj_3_1658()) return true;
    }
    return false;
  }

 inline bool jj_3_1657()
 {
    if (jj_done) return true;
    if (jj_3R_sequence_generator_start_with_option_6172_5_694()) return true;
    return false;
  }

 inline bool jj_3_1656()
 {
    if (jj_done) return true;
    if (jj_3R_common_sequence_generator_option_6150_5_693()) return true;
    return false;
  }

 inline bool jj_3R_common_sequence_generator_options_6144_5_560()
 {
    if (jj_done) return true;
    Token * xsp;
    if (jj_3_1656()) return true;
    while (true) {
      xsp = jj_scanpos;
      if (jj_3_1656()) { jj_scanpos = xsp; break; }
    }
    return false;
  }

 inline bool jj_3_1655()
 {
    if (jj_done) return true;
    if (jj_3R_common_sequence_generator_options_6144_5_560()) return true;
    return false;
  }


public: 
  void setErrorHandler(ErrorHandler *eh) {
    if (errorHandler) delete errorHandler;
    errorHandler = eh;
  }

  TokenManager *token_source = nullptr;
  CharStream   *jj_input_stream = nullptr;
  /** Current token. */
  Token        *token = nullptr;
  /** Next token. */
  Token        *jj_nt = nullptr;

private: 
  int           jj_ntk;
  JJCalls       jj_2_rtns[2183];
  bool          jj_rescan;
  int           jj_gc;
  Token        *jj_scanpos, *jj_lastpos;
  int           jj_la;
  /** Whether we are looking ahead. */
  bool          jj_lookingAhead;
  bool          jj_semLA;
  int           jj_gen;
  int           jj_la1[1];
  ErrorHandler *errorHandler = nullptr;

protected: 
  bool          hasError;


  /** Constructor with user supplied TokenManager. */
  Token *head; 
public: 
  SqlParser(TokenManager *tokenManager);
  virtual ~SqlParser();
void ReInit(TokenManager* tokenManager);
void clear();
Token * jj_consume_token(int kind);
bool  jj_scan_token(int kind);
Token * getNextToken();
Token * getToken(int index);
int jj_ntk_f();
private:
  int jj_kind;
protected:
  /** Generate ParseException. */
virtual void  parseError();
private:
  int  indent;	// trace indentation
  bool trace = false; // trace enabled if true

public:
  bool trace_enabled();
  void enable_tracing();
  void disable_tracing();
inline bool IsIdNonReservedWord() {
    auto kind = getToken(1)->kind;
    if (__builtin_expect(kind == regular_identifier, 1) || kind == delimited_identifier || kind == Unicode_delimited_identifier) return true;

    if (!(kind >= MIN_NON_RESERVED_WORD && kind <= MAX_NON_RESERVED_WORD)) return false;  // Not a nonreserved word.

    // Some special cases.
    switch (kind) {
      // Some contextual keywords
      case GROUP:
      case ORDER:
      case PARTITION:
        return getToken(2)->kind != BY;

      case LIMIT:
        return getToken(2)->kind != unsigned_integer;

      case ROWS:
        return getToken(2)->kind != BETWEEN;

      // Some builtin functions
      case TRIM:
      case POSITION:
      case MOD:
      case POWER:
      case RANK:
      case ROW_NUMBER:
      case FLOOR:
      case MIN:
      case MAX:
      case UPPER:
      case LOWER:
      case CARDINALITY:
      case ABS:
        return getToken(2)->kind != lparen;

      default:
        return true;
     }

     // Should never come here.
     return true;
  }

  inline bool SyncToSemicolon() {
    if (hasError || getToken(0)->kind != semicolon) {
      while (getToken(1)->kind != _EOF && getToken(1)->kind != semicolon) {
        getNextToken();
      }

      if (getToken(1)->kind == semicolon) {
        getNextToken();
      }

      hasError = false;
    }

    return true;
  }

  inline bool NotEof() {
    return getToken(1)->kind != _EOF;
  }

  void PushNode(Node* node) { jjtree.pushNode(node); }
  Node* PopNode() { return jjtree.popNode(); }

  void jjtreeOpenNodeScope(Node* node) {
    static_cast<AstNode*>(node)->beginToken = getToken(1);
  }

  void jjtreeCloseNodeScope(Node* node) {
    AstNode* astNode = static_cast<AstNode*>(node);
    astNode->endToken = getToken(0);
    Token* t = astNode->beginToken;

    // For some nodes, the node is opened after some children are already created. Reset the begin for those to be
    // the begin of the left-most child.
    if (astNode->NumChildren() > 0) {
      Token* t0 = astNode->GetChild(0)->beginToken;
      if (t0->beginLine < t->beginLine || (t0->beginLine == t->beginLine && t0->beginColumn < t->beginColumn)) {
        astNode->beginToken = t0;
      }
    }

    if (astNode->getId() == JJTUNSUPPORTED) {
      cout << "Unsupported feature used at: " << t->beginLine << ":" << t->beginColumn << " " << t->image  << "\n";
    }


     if (astNode->IsNegatableOperator()) {
        Token* t1 =  astNode->GetChild(0)->endToken;

        if (astNode->Kind() == JJTISNULL) {
           // IsNull -- see if the penultimate token is NOT
           while (t1 != null && t1->kind != IS) {
              t1 = t1->next;
           }

           if (t1->next->kind == NOT) {
              astNode->SetNegated(true);
           }
        }
        else if (astNode->NumChildren() > 1) {
            Token* t2 = astNode->GetChild(1)->beginToken;
            while (t1->next != null && t1->next != t2) {
               if (t1->kind == NOT) {
                  astNode->SetNegated(true);
                  break;
               }
               t1 = t1->next;
            }
        }
      }
      else  if (astNode->NumChildren() == 2 && astNode->IsOperator()) {
         // Hack locate the token just before the first token of the second operator
         Token* t1 = astNode->GetChild(0)->endToken;
         Token* t2 = astNode->GetChild(1)->beginToken;
         while (t1->next != nullptr && t1->next != t2) {
            t1 = t1->next;
         }
         astNode->SetOperator(t1->kind);
      }

      if (astNode->NumChildren() == 1 && astNode->IsOperator()) {
         astNode->SetOperator(astNode->beginToken->kind);
      }
  }

  JJTSqlParserState jjtree;
private:
  bool jj_done;
};
}
}
#endif
