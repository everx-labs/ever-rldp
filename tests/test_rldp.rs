use adnl::{
    adnl_node_compatibility_key, adnl_node_test_config, adnl_node_test_key,
    common::{KeyId, /* KeyOption,*/ serialize, serialize_append}, 
    node::{AdnlNode, AdnlNodeConfig}
};
use overlay::OverlayUtils;
use rldp::RldpNode;
use sha2::Digest;
use std::sync::Arc;
use ton_api::ton::{
    self,
    rpc::{overlay::Query as OverlayQuery, ton_node::{/*DownloadBlock,*/ DownloadZeroState}},
    ton_node::blockidext::BlockIdExt
};

include!("../common/src/log.rs");

// Overlay
const IP_TESTNET: &str = "77.47.135.100:6602";
const KEY_TAG: usize = 0;
const KEY_LOCAL: &str = "qW+Mj2NgxjByeMlA5YQ1I5mYj+ApHuhL/YSKmphaaK4=";
const KEY_TESTNET: &str = "7zZG2YCAZ4osBPP/rBcW0ddxCCP9nFGqBcRPeEokYGU=";

fn init_test() -> tokio::runtime::Runtime {
    init_test_log();
    tokio::runtime::Runtime::new().unwrap()
}

fn init_compatibility_test(ip: &str) -> (
    tokio::runtime::Runtime, 
    Arc<RldpNode>, 
    Arc<KeyId>,
    Arc<KeyId>
) {
    let mut rt = init_test();
    let config_ours = AdnlNodeConfig::from_json(
        adnl_node_test_config!(ip, adnl_node_test_key!(KEY_TAG, KEY_LOCAL)),
        true
    ).unwrap();
    let ours = config_ours.key_by_tag(KEY_TAG).unwrap().id().clone();    
/*
    AdnlNodeConfig::with_local_address_and_key_type(
        "0.0.0.0:4190",
        KeyOption::KEY_ED25519
    ).unwrap();
*/
    let config_peer = AdnlNodeConfig::from_json(
        adnl_node_test_config!(
            IP_TESTNET, 
            adnl_node_compatibility_key!(KEY_TAG, KEY_TESTNET)
        ),
        false
    ).unwrap();
    let adnl = rt.block_on(AdnlNode::with_config(config_ours)).unwrap();
    let rldp = RldpNode::with_adnl_node(adnl.clone()).unwrap();
    rt.block_on(AdnlNode::start(&adnl, vec![rldp.clone()])).unwrap();
    let peer = adnl.add_peer(
        &ours, 
        config_peer.ip_address(), 
        &config_peer.key_by_tag(KEY_TAG).unwrap()
    ).unwrap();
    (rt, rldp, peer, ours)
}

fn init_local_test() -> (tokio::runtime::Runtime, Arc<RldpNode>, Arc<RldpNode>) {
    let mut rt = init_test();
    let config1 = AdnlNodeConfig::from_json(
        adnl_node_test_config!("127.0.0.1:4192", adnl_node_test_key!(KEY_TAG, KEY_LOCAL)),
        true
    ).unwrap();
    let config2 = AdnlNodeConfig::from_json(
        adnl_node_test_config!("127.0.0.1:4193", adnl_node_test_key!(KEY_TAG, KEY_LOCAL)),
        true
    ).unwrap();
    let adnl1 = rt.block_on(AdnlNode::with_config(config1)).unwrap();
    let rldp1 = RldpNode::with_adnl_node(adnl1.clone()).unwrap();
    rt.block_on(AdnlNode::start(&adnl1, vec![rldp1.clone()])).unwrap();
    let adnl2 = rt.block_on(AdnlNode::with_config(config2)).unwrap();
    let rldp2 = RldpNode::with_adnl_node(adnl2.clone()).unwrap();
    rt.block_on(AdnlNode::start(&adnl2, vec![rldp2.clone()])).unwrap();
    let peer1 = adnl1.key_by_tag(KEY_TAG).unwrap(); 
    let peer2 = adnl2.key_by_tag(KEY_TAG).unwrap(); 
    adnl1.add_peer(peer1.id(), adnl2.ip_address(), &peer2).unwrap();
    adnl2.add_peer(peer2.id(), adnl1.ip_address(), &peer1).unwrap();
    (rt, rldp1, rldp2)
}

/*
async fn download_block(
    peer: &KeyId, 
    rldp: &Arc<RldpNode>, 
    root: &str, 
    file: &str, 
    seqno: i32, 
    prefix: &[u8]
) {
    let root_hash = base64::decode(root).unwrap();
    let file_hash = base64::decode(file).unwrap();
    let mut query = prefix.to_vec();
    let message = DownloadBlock {
        block: BlockIdExt {
            workchain: -1,
            shard: 0x8000000000000000u64 as i64,
            seqno,
            root_hash: ton::int256(arrayref::array_ref!(&root_hash, 0, 32).clone()),
            file_hash: ton::int256(arrayref::array_ref!(&file_hash, 0, 32).clone())
        }
    };
    serialize_append(&mut query, &message).unwrap();
    send_rldp_query(&peer, &rldp, &query, &format!("MC Block #{}", seqno), &file_hash)
        .await
}
*/

async fn send_rldp_query(
    rldp: &Arc<RldpNode>, 
    dst: &Arc<KeyId>, 
    src: &Arc<KeyId>, 
    query: &[u8], 
    msg: &str, 
    hash: &[u8]
) {
    let mut i: u32 = 0;
    let now = std::time::SystemTime::now();
    loop {
        i += 1;
        log::debug!("{}, attempt {}", msg, i);
        if let Some(answer) = rldp.query(dst, src, query, None).await.unwrap() {
            log::debug!("Totally received {} bytes", answer.len());
            assert!(sha2::Sha256::digest(&answer[..]).as_slice().eq(&hash[..]));
            break;
        }
    }
    log::debug!("Elapsed {}", now.elapsed().unwrap().as_millis());
}

#[test]
fn rldp_compatibility() {
    let (mut rt, rldp, peer, ours) = init_compatibility_test("0.0.0.0:4191");
    rt.block_on(
        async move { 

            // Overlay ID
            let file_hash = base64::decode(
                "XplPz01CXAps5qeSWUtxcyBfdAo5zVb1N979KLSKD24="
            ).unwrap();
            let overlay_id = OverlayUtils::calc_overlay_short_id(
                -1, 
                0x8000000000000000u64 as i64,
                arrayref::array_ref!(&file_hash, 0, 32)
            ).unwrap();
            let query = OverlayQuery {
                overlay: ton::int256(overlay_id.data().clone())
            };
            let query = serialize(&query).unwrap();

            // Zerostate
            let root_hash = base64::decode(
                "F6OpKZKqvqeFp6CQmFomXNMfMj2EnaUSOXN+Mh+wVWk="
            ).unwrap();
            let file_hash = base64::decode(
                "XplPz01CXAps5qeSWUtxcyBfdAo5zVb1N979KLSKD24="
            ).unwrap();
            let mut data = query.to_vec();
            let message = DownloadZeroState {
                block: BlockIdExt {
                    workchain: -1,
                    shard: 0x8000000000000000u64 as i64,
                    seqno: 0,
                    root_hash: ton::int256(arrayref::array_ref!(&root_hash, 0, 32).clone()),
                    file_hash: ton::int256(arrayref::array_ref!(&file_hash, 0, 32).clone())
                }
            };
            serialize_append(&mut data, &message).unwrap();
            send_rldp_query(&rldp, &peer, &ours, &data, "MC Zerostate", &file_hash).await;

/*
            // Blocks
            download_block(   
                &peer,
                &rldp, 
                "34qYxAeiFxFZzBSwXpcXqhd8RfKrdml7Z4HWXo98CU0=",
                "aDLVLzppSkbkCtDaSiGjCGBZ/6Bd5YnCvR/TTtXLGDo=",
                2343424,
                &query
            ).await;

            let root_hash = base64::decode(
                "34qYxAeiFxFZzBSwXpcXqhd8RfKrdml7Z4HWXo98CU0="
            ).unwrap();
            let file_hash = base64::decode(
                "aDLVLzppSkbkCtDaSiGjCGBZ/6Bd5YnCvR/TTtXLGDo="
            ).unwrap();
            let mut data = serialize(&query).unwrap();
            let message = DownloadBlock {
                block: BlockIdExt {
                    workchain: -1,
                    shard: 0x8000000000000000u64 as i64,
                    seqno: 2343424,
                    root_hash: ton::int256(arrayref::array_ref!(&root_hash, 0, 32).clone()),
                    file_hash: ton::int256(arrayref::array_ref!(&file_hash, 0, 32).clone())
                }
            };
            serialize_append(&mut data, &message).unwrap();
            send_rldp_query(&peer, &rldp, &data, "MC Block #2343424", &file_hash).await;

            let root_hash = base64::decode(
                "dAzFP+hpQ1FpG+GT+vsqAYhnhhFOUJwTd+DWYsGuT8U"
            ).unwrap();
            let file_hash = base64::decode(
                "x8iHiYhMWR+CQcnW4YVmOcDRsuKRlCJI8bxW0i2hdZg="
            ).unwrap();
            let mut data = serialize(&query).unwrap();
            let message = DownloadBlock {
                block: BlockIdExt {
                    workchain: -1,
                    shard: 0x8000000000000000u64 as i64,
                    seqno: 2369389,
                    root_hash: ton::int256(arrayref::array_ref!(&root_hash, 0, 32).clone()),
                    file_hash: ton::int256(arrayref::array_ref!(&file_hash, 0, 32).clone())
                }
            };
            serialize_append(&mut data, &message).unwrap();
            send_rldp_query(&peer, &rldp, &data, "MC Block #2369389", &file_hash).await;

            let root_hash = base64::decode(
                "sx8VBzPqUrd6PNsAvUvrm7YmMIRP4eYXLETuGWzTqvo="
            ).unwrap();
            let file_hash = base64::decode(
                "av22mPrOMKJLH7OOu7wV8POxmDAIjXT7qBR/ZNguIxs="
            ).unwrap();
            let mut data = serialize(&query).unwrap();
            let message = DownloadBlock {
                block: BlockIdExt {
                    workchain: -1,
                    shard: 0x8000000000000000u64 as i64,
                    seqno: 2331956,
                    root_hash: ton::int256(arrayref::array_ref!(&root_hash, 0, 32).clone()),
                    file_hash: ton::int256(arrayref::array_ref!(&file_hash, 0, 32).clone())
                }
            };
            serialize_append(&mut data, &message).unwrap();
            send_rldp_query(&peer, &rldp, &data, "MC Block #2331956", &file_hash).await;
*/

    	}
    )
}

#[test]
fn rldp_session() {
    let (mut rt, _rldp1, _rldp2) = init_local_test();
    rt.block_on(
        async move {
//            let data = [1, 2, 3, 4, 5];
//            assert!(rldp1.send_query(&peer2, &data).await.unwrap());
//            assert!(rldp2.send_query(&peer1, &data).await.unwrap());
    	}
    )
}

	