@echo off
setlocal ENABLEDELAYEDEXPANSION
for /f "usebackq" %%a in (`type testSend\user_hash.txt`) do set user_hash=%%a
set sales_id=100
set item_id=203866
set query_string=ATMEGA16-16AU

for /l %%a in (1,1,1000) do (
    echo call :tests
    testSend search_item_name_dms_h "{'user_hash': '%user_hash%', 'query_string': 'abc!random!'}"
    ping -n 10 127.0.0.1 > nul
)
goto :eof

:tests
REM testSend purgeQueue timestamp "{'user_hash': '%user_hash%'}"
REM testSend timestamp "{'user_hash': '%user_hash%'}"

testSend add_dlv_mode "{'user_hash': '%user_hash%'}"
testSend address_add "{'user_hash': '%user_hash%'}"
testSend application_area_list "{'user_hash': '%user_hash%'}"
testSend calc_dlv_amount "{'user_hash': '%user_hash%'}"
testSend claim_create "{'user_hash': '%user_hash%'}"
testSend claim_info "{'user_hash': '%user_hash%'}"
testSend create_invoice "{'user_hash': '%user_hash%'}"
testSend delivery_prognosis "{'user_hash': '%user_hash%', 'item_id': '%item_id%'}"
testSend info_cust "{'user_hash': '%user_hash%'}"
testSend info_cust_balance "{'user_hash': '%user_hash%'}"
testSend info_cust_contracts "{'user_hash': '%user_hash%'}"
testSend info_cust_limits "{'user_hash': '%user_hash%'}"
testSend info_cust_trans "{'user_hash': '%user_hash%'}"
testSend invoice_paym "{'user_hash': '%user_hash%'}"
testSend item_info "{'user_hash': '%user_hash%', 'item_id': '%item_id%'}"
testSend make_order_ext "{'user_hash': '%user_hash%'}"
testSend multiplicate_values "{'user_hash': '%user_hash%', 'value1': '10', 'value2': '14'}"
testSend place_info "{'user_hash': '%user_hash%'}"
testSend place_lines "{'user_hash': '%user_hash%'}"
testSend quotation_handle_header "{'user_hash': '%user_hash%'}"
testSend quotation_info "{'user_hash': '%user_hash%'}"
testSend quotation_lines "{'user_hash': '%user_hash%'}"
testSend sales_close_reason_list "{'user_hash': '%user_hash%'}"
testSend sales_delivery "{'user_hash': '%user_hash%', 'sales_id': '%sales_id%'}"
testSend sales_handle_add "{'user_hash': '%user_hash%'}"
testSend sales_handle_edit "{'user_hash': '%user_hash%', 'sales_id': '%sales_id%'}"
testSend sales_info "{'user_hash': '%user_hash%'}"
testSend sales_lines "{'user_hash': '%user_hash%'}"
testSend sales_status_list "{'user_hash': '%user_hash%'}"
testSend search_item_name_dms_h "{'user_hash': '%user_hash%', 'query_string': '%query_string%'}"
testSend search_item_name_h "{'user_hash': '%user_hash%', 'query_string': '%query_string%'}"
testSend timestamp "{'user_hash': '%user_hash%'}"
testSend user_info "{'user_hash': '%user_hash%'}"
testSend user_list "{'user_hash': '%user_hash%'}"
