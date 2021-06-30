#!/usr/bin/env ts-node

import { exec } from 'child_process';
import * as cliProgress from 'cli-progress';
import { Command } from 'commander';
import { createHash } from 'crypto';
import { config as dotenvConfig } from 'dotenv';
import { lookpath } from 'lookpath';
import randomWords from 'random-words';

const getHost = (account) => {
    const hostname = randomWords({ min: 1, max: 2, join: ' ', formatter: (word, index) => {
        return index === 0 ? word.slice(0, 1).toUpperCase().concat(word.slice(1)) : word;
    } });
    const inventoryId = createHash('md5').update(hostname).digest('hex');
    const insightsId = createHash('md5').update(inventoryId).digest('hex');

    return {
        timestamp: new Date().toISOString(),
        host: {
            account,
            created: new Date().toISOString(),
            updated: new Date().toISOString(), //'2011-12-03T10:15:30+01:00',
            display_name: hostname,
            tags: [],
            reporter: 'puptoo',
            id: inventoryId,
            insights_id: insightsId,
            system_profile: {
                arch: 'x86_64',
                bios_release_date: 'string',
                bios_vendor: 'string',
                bios_version: 'string',
                cloud_provider: 'string',
                cores_per_socket: 2,
                cpu_flags: [
                    'string'
                ],
                disk_devices: [
                    {
                        device: '/dev/fdd0',
                        label: 'string',
                        mount_point: '/mnt/remote_nfs_shares',
                        options: {
                            ro: true,
                            uid: '0'
                        },
                        type: 'ext3'
                    }
                ],
                enabled_services: [
                    'string'
                ],
                infrastructure_type: 'string',
                infrastructure_vendor: 'string',
                insights_client_version: 'string',
                insights_egg_version: 'string',
                installed_packages: [
                    '0:krb5-libs-1.16.1-23.fc29.i686'
                ],
                installed_products: [
                    {
                        id: '71',
                        name: 'string',
                        status: 'Subscribed'
                    }
                ],
                installed_services: [
                    'string'
                ],
                katello_agent_running: true,
                kernel_modules: [
                    'string'
                ],
                last_boot_time: '2019-11-20T14:42:38.905Z',
                network_interfaces: [
                    {
                        ipv4_addresses: [
                            '198.51.100.42'
                        ],
                        ipv6_addresses: [
                            '2001:0db8:5b96:0000:0000:426f:8e17:642a'
                        ],
                        mac_address: '00:00:00:00:00:00',
                        mtu: 0,
                        name: 'eth0',
                        state: 'UP',
                        type: 'ether'
                    }
                ],
                number_of_cpus: 2,
                number_of_sockets: 2,
                os_kernel_version: 'string',
                os_release: 'string',
                running_processes: [
                    'string'
                ],
                satellite_managed: true,
                subscription_auto_attach: 'string',
                subscription_status: 'string',
                system_memory_bytes: 0,
                yum_repos: [
                    {
                        baseurl: 'string',
                        enabled: true,
                        gpgcheck: true,
                        name: 'string'
                    }
                ]
            }
        }
    };
};

const run = async () => {

    const kafkacat = await lookpath('kafkacat');

    if (!kafkacat) {
        console.error('kafkacat is required to run this command, please install it and have it available on your path');
        process.exit(1);
    }

    const program = new Command();

    program
    .description('Pushes random hosts to local kafka for testing purposes')
    .option(
        '-a, --account <account-number>',
        'Account number to use, also loaded from env.INSIGHTS_ACCOUNT or from file .push-host.env'
    )
    .option(
        '-k, --kafka <kafka-server:kafka-port>',
        'Kafka server to pass the data',
        'localhost:9092'
    )
    .option(
        '-t, --topic <topic>',
        'Topic to send the data',
        'platform.inventory.events'
    )
    .option<number>(
        '-ac, --alert-count <alert-count>',
        'Number of alerts to generate',
        (value: string) => parseInt(value) || 20,
        20
    )
    .option<number>(
        '-s, --sleep <sleep-time-between-each-ms>',
        'Sleep time (ms) between each data to avoid having them all at the same time',
        (value: string) => parseInt(value) || 100,
        100
    ).option(
        '--ssl',
        'If should connect using security.protocol=ssl (will also set the verification to false)',
        false
    );

    program.parse();

    interface Params {
        account?: string;
        kafka: string;
        topic: string;
        alertCount: number;
        sleep: number;
        ssl: boolean;
    }

    const params = program as unknown as Params;

    dotenvConfig({
        path: './.push-host.env'
    });

    if (!params.account) {
        if (!process.env.INSIGHTS_ACCOUNT) {
            console.error('-account was not used and INSIGHTS_ACCOUNT env was not found, exiting');
            process.exit(1);
        }

        params.account = process.env.INSIGHTS_ACCOUNT;
    }

    console.info('Using arguments:', {
        account: params.account,
        kafka: params.kafka,
        topic: params.topic,
        alertCount: params.alertCount,
        sleep: params.sleep,
        ssl: params.ssl
    });

    const sslArguments = '-X security.protocol=ssl -X enable.ssl.certificate.verification=false';

    const kafkacatProcess = exec(
        `${kafkacat} -P -t ${params.topic} -b ${params.kafka} ${params.ssl ? sslArguments : ''} -H "event_type=updated"`
    );

    if (kafkacatProcess.stdin) {
        const progressBar = new cliProgress.SingleBar({}, cliProgress.Presets.shades_classic);
        progressBar.start(params.alertCount, 0);
        for (let i = 0; i < params.alertCount; ++i) {
            kafkacatProcess.stdin.write(JSON.stringify(getHost(params.account)));
            kafkacatProcess.stdin.write('\n');
            progressBar.increment();
            await new Promise(resolve => setTimeout(resolve, params.sleep));
        }

        progressBar.stop();
        kafkacatProcess.stdin.end();
    }

    kafkacatProcess.stdout?.pipe(process.stdout);
    kafkacatProcess.stderr?.pipe(process.stderr);

};

run();
