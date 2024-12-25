pub mod tmtc_generic_c2a {
    use crate::{
        fop, registry::{CommandRegistry, TelemetryRegistry}
    };

    tonic::include_proto!("tmtc_generic_c2a");

    pub const FILE_DESCRIPTOR_SET: &[u8] = tonic::include_file_descriptor_set!("tmtc_generic_c2a");

    pub struct Service {
        satellite_schema: SatelliteSchema,
    }

    impl Service {
        pub fn new(
            tlm_registry: &TelemetryRegistry,
            cmd_registry: &CommandRegistry,
            tc_scid: u16,
            aos_scid: u8,
        ) -> anyhow::Result<Self> {
            let telemetry_channels = fop::tlmcmd::build_telemetry_channel_schema_map()
                .into_iter()
                .chain(
                    tlm_registry
                        .build_telemetry_channel_schema_map(),
                )
                .collect();
            let telemetry_components = fop::tlmcmd::build_telemetry_component_schema_map()
                .into_iter()
                .chain(
                    tlm_registry
                        .build_telemetry_component_schema_map(),
                )
                .collect();
            let command_prefixes = cmd_registry.build_command_prefix_schema_map();
            let command_components = cmd_registry.build_command_component_schema_map();
            let tc_scid = tc_scid as u32;
            let aos_scid = aos_scid as u32;
            let satellite_schema = SatelliteSchema {
                telemetry_channels,
                telemetry_components,
                command_prefixes,
                command_components,
                tc_scid,
                aos_scid,
            };
            Ok(Self { satellite_schema })
        }
    }

    #[tonic::async_trait]
    impl tmtc_generic_c2a_server::TmtcGenericC2a for Service {
        async fn get_satellite_schema(
            &self,
            _request: tonic::Request<GetSatelliteSchemaRequest>,
        ) -> Result<tonic::Response<GetSateliteSchemaResponse>, tonic::Status> {
            Ok(tonic::Response::new(GetSateliteSchemaResponse {
                satellite_schema: Some(self.satellite_schema.clone()),
            }))
        }
    }
}
