-- One consistent statement; only maintained market data and policy, never wallets.
WITH fee_schedules(key, rate, exponent) AS (VALUES
 ('sports','0.03','1'), ('crypto','0.072','1'), ('finance','0.04','1'),
 ('politics','0.04','1'), ('economics','0.03','0.5'), ('culture','0.05','1'),
 ('weather','0.025','0.5'), ('other_general','0.2','2'), ('mentions','0.25','2'),
 ('tech','0.04','1'), ('geopolitics','0','1')
)
SELECT jsonb_build_object(
 'market_id',m.id,'question',m.question,'market_volume',m.volume::text,'event_volume',e.volume::text,
 'tags',COALESCE((SELECT jsonb_agg(t.label ORDER BY t.id) FROM market_data_event_tags et JOIN market_data_tag t ON t.id=et.tag_id WHERE et.event_id=e.id),'[]'::jsonb),
 'eligible',false,'token_id_yes',m.token_id_yes,'token_id_no',m.token_id_no,
 'active',m.active,'closed',m.closed,'accepting_orders',m.accepting_orders,'auto_archived',m.auto_archived,
 'min_tick_size',m.min_tick_size::text,'min_order_size',m.min_order_size::text,'neg_risk',m.neg_risk,
 'fees_enabled',m.fees_enabled,'fee_verification_status',m.fee_verification_status,
 'fee_schedule',CASE
   WHEN m.fee_schedule_rate IS NOT NULL AND m.fee_schedule_exponent IS NOT NULL
   THEN jsonb_build_object('rate',m.fee_schedule_rate::text,'exponent',m.fee_schedule_exponent::text)
   ELSE (SELECT jsonb_build_object('rate',f.rate,'exponent',f.exponent) FROM fee_schedules f
     WHERE f.key IN (lower(trim(m.fee_schedule_key)), lower(trim(m.fee_category)))
     ORDER BY (f.key=lower(trim(m.fee_schedule_key))) DESC NULLS LAST LIMIT 1) END,
 'has_disputed_resolution',EXISTS(SELECT 1 FROM market_data_resolution d WHERE (d.market_id=m.id OR d.market_id_external=m.id) AND d.status='disputed'),
 'settled_request_blocks',COALESCE((SELECT jsonb_object_agg(s.uma_request_id,s.block) FROM
   (SELECT uma_request_id,MAX(COALESCE(settle_block_number,0)) AS block FROM market_data_resolution
    WHERE (market_id=m.id OR market_id_external=m.id) AND status='settled' AND uma_request_id IS NOT NULL
      AND uma_request_id<>'' AND settle_block_number IS NOT NULL GROUP BY uma_request_id) s),'{}'::jsonb),
 'existing_order_count',0,'valuation',NULL,
 'resolution',CASE WHEN r.id IS NULL THEN NULL ELSE jsonb_build_object(
   'id',r.id,'request_id',r.uma_request_id,'status',r.status,'proposed_price',r.proposed_price::text,
   'propose_time_ms',(EXTRACT(EPOCH FROM r.propose_time)*1000)::bigint,
   'block_number',r.block_number,'dispute_block_number',r.dispute_block_number,'settle_block_number',r.settle_block_number,
   'disputed',r.status='disputed' OR r.dispute_timestamp IS NOT NULL,
   'settled',r.status='settled' OR r.settle_timestamp IS NOT NULL) END
) AS market,
(SELECT jsonb_build_object(
 'manual_trade_shutdown_enabled',c.manual_trade_shutdown_enabled,
 'strategy_enabled',EXISTS(SELECT 1 FROM strategy_tradelinedefinition WHERE strategy_key='post_propose_winner' AND is_active),
 'valuation_key',(SELECT valuation_key FROM strategy_tradelinedefinition WHERE line_key='post_propose_winner.default' LIMIT 1),
 'max_ask_price',c.max_ask_price::text,'max_orders_per_market',c.max_orders_per_market,'ev_threshold',c.ev_threshold::text,
 'order_size_usd',c.order_size_usd::text,'low_price_order_size_usd',c.low_price_order_size_usd::text,
 'low_depth_099_order_size_usd',c.low_depth_099_order_size_usd::text)
 FROM market_data_autotradeconfig c ORDER BY c.id LIMIT 1) AS policy
FROM market_data_market m JOIN market_data_event e ON e.id=m.event_id
LEFT JOIN LATERAL (SELECT id,uma_request_id,status,proposed_price,propose_time,block_number,
  dispute_block_number,settle_block_number,dispute_timestamp,settle_timestamp FROM market_data_resolution
  WHERE market_id=m.id OR market_id_external=m.id ORDER BY propose_time DESC,id DESC LIMIT 1) r ON true
WHERE m.id=$1
