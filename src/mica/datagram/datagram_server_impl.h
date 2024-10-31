#pragma once
#ifndef MICA_DATAGRAM_DATAGRAM_SERVER_IMPL_H_
#define MICA_DATAGRAM_DATAGRAM_SERVER_IMPL_H_

namespace mica {
namespace datagram {
template <class StaticConfig>
DatagramServer<StaticConfig>::DatagramServer(const ::mica::util::Config& config,
                                             Processor* processor,
                                             Network* network,
                                             DirectoryClient* dir_client)
    : config_(config),
      processor_(processor),
      network_(network),
      dir_client_(dir_client) {
  // assert(::mica::util::lcore.lcore_count() <= StaticConfig::kMaxLCoreCount);

  stopwatch_.init_start();

  directory_refresh_interval_ = ::mica::util::safe_cast<uint32_t>(
      config.get("directory_refresh_interval").get_uint64(1));
  directory_refresh_lcore_ = ::mica::util::safe_cast<uint16_t>(
      config.get("directory_refresh_lcore")
          .get_uint64(::mica::util::lcore.lcore_count() - 1));

  rebalance_interval_ = ::mica::util::safe_cast<uint32_t>(
      config.get("rebalance_interval").get_uint64(0));

  flush_status_report_ = config.get("flush_status_report").get_bool(true);

//  generate_server_info();

//  stopwatch_.init_end();

  uint64_t core_count = config.get("lcores").get_uint64();

    for (int t = 0; t < core_count; t++) {
      int result;
      result = doca_init(t);
      if (result != DOCA_SUCCESS) {
              printf("lcore %2zu: DOCA init failure\n", ::mica::util::lcore.lcore_id());
      }
      result = doca_init_recv_buf(t);
      if (result != DOCA_SUCCESS) {
              printf("lcore %2zu: DOCA recv_buf init failure\n", ::mica::util::lcore.lcore_id());
      }
    }

  /*int result;
  result = doca_init();
  if (result != DOCA_SUCCESS) {
          printf("lcore %2zu: DOCA init failure\n", ::mica::util::lcore.lcore_id());
  }
  result = doca_init_recv_buf();
  if (result != DOCA_SUCCESS) {
          printf("lcore %2zu: DOCA recv_buf init failure\n", ::mica::util::lcore.lcore_id());
  }*/

  generate_server_info();

  stopwatch_.init_end();

}

bool enable_printf = false;
bool enable_printf_no_offload = false;
//uint64_t total_time_end;
uint64_t in_batch;
//uint16_t out_batch;
//std::vector<uint64_t> begin_times;
uint64_t parse_start_time;
uint64_t parse_end_time;
uint64_t tot_parse_time = 0;
uint64_t dma_start_time;
uint64_t dma_end_time;
uint64_t tot_dma_time = 0;
uint64_t wait_start_time;
uint64_t wait_end_time;
uint64_t tot_wait_time = 0;
uint64_t response_start_time1;
uint64_t response_end_time1;
uint64_t tot_response_time1 = 0;
uint64_t response_start_time2;
uint64_t response_end_time2;
uint64_t tot_response_time2 = 0;
uint64_t tot_server_time = 0;
//std::vector<uint64_t> begin_send_times;
//std::vector<uint64_t> begin_wait_times;
//std::vector<uint64_t> host_start_times;
//std::vector<uint64_t> send_path_times;
::mica::util::Stopwatch ra_stopwatch_;

uint64_t no_offload_start_time;
uint64_t tot_no_offload_time = 0;
uint64_t out_batch = 0;
/*uint64_t parse_req_batch_start_time = 0;
uint64_t parse_req_batch_end_time = 0;
uint64_t tot_parse_req_batch_time = 0;
uint64_t retire_start_time = 0;
uint64_t retire_end_time = 0;
uint64_t tot_retire_time = 0;*/

// Variables for doca dma operation
//struct program_core_objects state = {0};
std::vector<struct program_core_objects> states(8, {0});
//struct doca_event event = {0};
std::vector<struct doca_event> events(8, {0});
//struct doca_dma_job_memcpy dma_job = {0};
std::vector<struct doca_dma_job_memcpy> dma_jobs(8, {0});
//struct doca_dma *dma_ctx;
std::vector<struct doca_dma*> dma_ctxs(8);
//struct doca_buf *host_dma_buf;
std::vector<doca_buf*> host_dma_bufs(8);
//struct doca_buf *dpu_dma_buf;
std::vector<doca_buf*> dpu_dma_bufs(8);
//struct doca_mmap *remote_mmap;
std::vector<doca_mmap*> remote_mmaps(8);
//char *dpu_dma_send_buf;
std::vector<char*> dpu_dma_send_bufs(8);
size_t dpu_dma_send_buf_len;
size_t dpu_dma_recv_buf_len;
//char *dpu_dma_recv_buf;
std::vector<char*> dpu_dma_recv_bufs(8);
//char *remote_addr = NULL;
std::vector<char*> remote_addrs(8, NULL);

template <class StaticConfig>
void  DatagramServer<StaticConfig>::sig_handler(int signum) {
        printf("\nFreeing doca resources\n");
	doca_argp_destroy();
        uint16_t lcore_count =
      		static_cast<uint16_t>(::mica::util::lcore.lcore_count());
	for (uint16_t i = 0; i < lcore_count; i++) {
		if (!rte_lcore_is_enabled(static_cast<uint8_t>(i))) continue;
		doca_buf_refcount_rm(dpu_dma_bufs[i], NULL);
		doca_buf_refcount_rm(host_dma_bufs[i], NULL);
		doca_mmap_destroy(remote_mmaps[i]);
		free(dpu_dma_send_bufs[i]);
		free(dpu_dma_recv_bufs[i]);
		dma_cleanup(&states[i], dma_ctxs[i]);
	}
        exit(signum);
}

std::vector<program_core_objects> state_1(8, {0});

template <class StaticConfig>
doca_error_t DatagramServer<StaticConfig>::doca_init_recv_buf(uint16_t lcore_id) {
	struct doca_pci_bdf pcie_dev_1;
	struct dma_config dma_conf_1 = {0};
	doca_error_t res_1;
	//struct program_core_objects state_1 = {0};
	//size_t lcore_id = ::mica::util::lcore.lcore_id();

	char desc_txt[30];
	snprintf(desc_txt, 30, "dpu_desc%d.txt", (size_t)lcore_id);
	char buf_txt[30];
	snprintf(buf_txt, 30, "dpu_buf%d.txt", (size_t)lcore_id);
	strcpy(dma_conf_1.pci_address, "03:00.0");
	strcpy(dma_conf_1.export_desc_path, desc_txt);
	strcpy(dma_conf_1.buf_info_path, buf_txt);

	res_1 = doca_argp_init("dma_copy_dpu_2", &dma_conf_1);
	if (res_1 != DOCA_SUCCESS) {
		DOCA_LOG_ERR("Failed to init ARGP resources: %s", doca_get_error_string(res_1));
		return res_1;
	}

        res_1 = register_dma_params();
        if (res_1 != DOCA_SUCCESS) {
                DOCA_LOG_ERR("Failed to register DMA sample parameters: %s", doca_get_error_string(res_1));
                //return EXIT_FAILURE;
                return res_1;
        }

        res_1 = parse_pci_addr(dma_conf_1.pci_address, &pcie_dev_1);
        if (res_1 != DOCA_SUCCESS) {
                DOCA_LOG_ERR("Failed to parse pci address: %s", doca_get_error_string(res_1));
                doca_argp_destroy();
                //return EXIT_FAILURE;
                return res_1;
        }

	/* Open the relevant DOCA device */
        res_1 = open_doca_device_with_pci(&pcie_dev_1, &dma_jobs_is_supported, &state_1[lcore_id].dev);
        if (res_1 != DOCA_SUCCESS)
                return res_1;


	/* Init all DOCA core objects */
        res_1 = host_init_core_objects(&state_1[lcore_id]);
        if (res_1 != DOCA_SUCCESS) {
                host_destroy_core_objects(&state_1[lcore_id]);
                return res_1;
        }

        /* Copy all relevant information into local buffers */
	char *export_desc_1;
	size_t export_desc_len_1;

	dpu_dma_recv_buf_len = sizeof(MiniResponse) * StaticConfig::kTXBurst + 1;
        //dpu_dma_recv_buf = (char *)malloc(dpu_dma_recv_buf_len);
	int r = posix_memalign((void**)&dpu_dma_recv_bufs[lcore_id], 64, dpu_dma_recv_buf_len);
        //if (dpu_dma_recv_buf == NULL) {
	if ( r != 0) {
                DOCA_LOG_ERR("dpu_dma_recv_buf allocation failed");
                doca_argp_destroy();
                return DOCA_ERROR_NO_MEMORY;
        }
        memset(dpu_dma_recv_bufs[lcore_id], 0, dpu_dma_recv_buf_len);
	//printf("lcore %" PRIu16 ": dpu_dma_recv_bufs[%" PRIu16 "] addr: %p\n", lcore_id, lcore_id, dpu_dma_recv_bufs[lcore_id]);
	//printf("lcore %2zu: dpu_dma_recv_buf_len = %d\n", ::mica::util::lcore.lcore_id(), dpu_dma_recv_buf_len);

	/* Populate the memory map with the allocated memory */
	res_1 = doca_mmap_populate(state_1[lcore_id].mmap, dpu_dma_recv_bufs[lcore_id], dpu_dma_recv_buf_len, PAGE_SIZE, NULL, NULL);
	if (res_1 != DOCA_SUCCESS) {
		host_destroy_core_objects(&state_1[lcore_id]);
		return res_1;
	}

	/* Export DOCA mmap to enable DMA*/
	res_1 = doca_mmap_export(state_1[lcore_id].mmap, state_1[lcore_id].dev, (void **)&export_desc_1, &export_desc_len_1);
	if (res_1 != DOCA_SUCCESS) {
		host_destroy_core_objects(&state_1[lcore_id]);
		return res_1;
	}

	/* Saves the export desc and buffer info to files, it is the user responsibility to transfer them to the dpu */
	res_1 = save_config_info_to_files(export_desc_1, export_desc_len_1, dpu_dma_recv_bufs[lcore_id], dpu_dma_recv_buf_len,
					   dma_conf_1.export_desc_path, dma_conf_1.buf_info_path);
	if (res_1 != DOCA_SUCCESS) {
		free(export_desc_1);
		host_destroy_core_objects(&state_1[lcore_id]);
		return res_1;
	}

	DOCA_LOG_INFO("lcore %2zu: DOCA recv_buf init successful", ::mica::util::lcore.lcore_id());

	/*while(1) {
		printf("Memory content %" PRIu8 "\n", dpu_dma_recv_buf[0]);
		sleep(15);
	}*/

	return res_1;
}

template <class StaticConfig>
doca_error_t DatagramServer<StaticConfig>::doca_init(uint16_t lcore_id) {
        struct dma_config dma_conf = {0};
        struct doca_pci_bdf pcie_dev;
        doca_error_t result;
	//size_t lcore_id = ::mica::util::lcore.lcore_id();

        char desc_txt[30];
        snprintf(desc_txt, 30, "host_desc%d.txt", (size_t)lcore_id);
        char buf_txt[30];
        snprintf(buf_txt, 30, "host_buf%d.txt", (size_t)lcore_id);
        strcpy(dma_conf.pci_address, "03:00.0");
        strcpy(dma_conf.export_desc_path, desc_txt);
        strcpy(dma_conf.buf_info_path, buf_txt);

        result = doca_argp_init("dma_copy_dpu", &dma_conf);
        if (result != DOCA_SUCCESS) {
                DOCA_LOG_ERR("Failed to init ARGP resources: %s", doca_get_error_string(result));
                //return EXIT_FAILURE;
                return result;
        }
        result = register_dma_params();
        if (result != DOCA_SUCCESS) {
                DOCA_LOG_ERR("Failed to register DMA sample parameters: %s", doca_get_error_string(result));
                //return EXIT_FAILURE;
                return result;
        }
        result = parse_pci_addr(dma_conf.pci_address, &pcie_dev);
        if (result != DOCA_SUCCESS) {
                DOCA_LOG_ERR("Failed to parse pci address: %s", doca_get_error_string(result));
                doca_argp_destroy();
                //return EXIT_FAILURE;
                return result;
        }

	/*struct program_core_objects state = {0};
	struct doca_event event = {0};
	struct doca_dma_job_memcpy dma_job = {0};
	struct doca_dma *dma_ctx;
	struct doca_buf *host_dma_buf;
	struct doca_buf *dpu_dma_buf;
	struct doca_mmap *remote_mmap;*/
	struct timespec ts = {0};
	uint32_t max_chunks = 2;
	char export_desc[1024] = {0};
	//char *remote_addr = NULL;
	//char *dpu_buffer;
	size_t remote_addr_len = 0, export_desc_len = 0;

	result = doca_dma_create(&dma_ctxs[lcore_id]);
	if (result != DOCA_SUCCESS) {
		DOCA_LOG_ERR("Unable to create DMA engine: %s", doca_get_error_string(result));
		return result;
	}

	states[lcore_id].ctx = doca_dma_as_ctx(dma_ctxs[lcore_id]);

	result = open_doca_device_with_pci(&pcie_dev, &dma_jobs_is_supported, &states[lcore_id].dev);
	if (result != DOCA_SUCCESS) {
		doca_dma_destroy(dma_ctxs[lcore_id]);
		return result;
	}

	result = init_core_objects(&states[lcore_id], DOCA_BUF_EXTENSION_NONE, WORKQ_DEPTH, max_chunks);
	if (result != DOCA_SUCCESS) {
		dma_cleanup(&states[lcore_id], dma_ctxs[lcore_id]);
		return result;
	}

	/* Copy all relevant information into local buffers */
	save_config_info_to_buffers(dma_conf.export_desc_path, dma_conf.buf_info_path, export_desc, &export_desc_len,
				    &remote_addrs[lcore_id], &remote_addr_len);

	dpu_dma_send_buf_len = remote_addr_len;
	//dpu_dma_send_buf = (char *)malloc(dpu_dma_send_buf_len);
	//int r = posix_memalign((void**)&dpu_dma_send_buf, 64, dpu_dma_send_buf_len);
	int r = posix_memalign((void**)&dpu_dma_send_bufs[lcore_id], 64, dpu_dma_send_buf_len);
	//if (dpu_dma_send_buf == NULL) {
	if ( r != 0) {
		DOCA_LOG_ERR("Failed to allocate buffer memory");
		dma_cleanup(&states[lcore_id], dma_ctxs[lcore_id]);
		return DOCA_ERROR_NO_MEMORY;
	}
	memset(dpu_dma_send_bufs[lcore_id], 0, dpu_dma_send_buf_len);
	//printf("lcore %2zu: dpu_dma_send_bufs[%d] addr %p\n", lcore_id, lcore_id, dpu_dma_send_bufs[lcore_id]);
	//memset(dpu_buffer, '1', dpu_dma_buf_size);
	//memset(dpu_buffer, '2', 1);
	//memset(dpu_buffer+1, '1', dpu_dma_buf_size-1);

	result = doca_mmap_populate(states[lcore_id].mmap, dpu_dma_send_bufs[lcore_id], dpu_dma_send_buf_len, PAGE_SIZE, NULL, NULL);
	if (result != DOCA_SUCCESS) {
		free(dpu_dma_send_bufs[lcore_id]);
		dma_cleanup(&states[lcore_id], dma_ctxs[lcore_id]);
		return result;
	}

	/* Create a local DOCA mmap from exported data */
	result = doca_mmap_create_from_export(NULL, (const void *)export_desc, export_desc_len, states[lcore_id].dev,
					   &remote_mmaps[lcore_id]);
	if (result != DOCA_SUCCESS) {
		free(dpu_dma_send_bufs[lcore_id]);
		dma_cleanup(&states[lcore_id], dma_ctxs[lcore_id]);
		return result;
	}

	/* Construct DOCA buffer for each address range */
	result = doca_buf_inventory_buf_by_addr(states[lcore_id].buf_inv, remote_mmaps[lcore_id], remote_addrs[lcore_id], remote_addr_len, &host_dma_bufs[lcore_id]);
	if (result != DOCA_SUCCESS) {
		DOCA_LOG_ERR("Unable to acquire DOCA buffer representing remote buffer: %s",
			     doca_get_error_string(result));
		doca_mmap_destroy(remote_mmaps[lcore_id]);
		free(dpu_dma_send_bufs[lcore_id]);
		dma_cleanup(&states[lcore_id], dma_ctxs[lcore_id]);
		return result;
	}

	/* Construct DOCA buffer for each address range */
	result = doca_buf_inventory_buf_by_addr(states[lcore_id].buf_inv, states[lcore_id].mmap, dpu_dma_send_bufs[lcore_id], dpu_dma_send_buf_len, &dpu_dma_bufs[lcore_id]);
	if (result != DOCA_SUCCESS) {
		DOCA_LOG_ERR("Unable to acquire DOCA buffer representing destination buffer: %s",
			     doca_get_error_string(result));
		doca_buf_refcount_rm(dpu_dma_bufs[lcore_id], NULL);
		doca_mmap_destroy(remote_mmaps[lcore_id]);
		free(dpu_dma_send_bufs[lcore_id]);
		dma_cleanup(&states[lcore_id], dma_ctxs[lcore_id]);
		return result;
	}

	/* Construct DMA job */
	dma_jobs[lcore_id].base.type = DOCA_DMA_JOB_MEMCPY;
	dma_jobs[lcore_id].base.flags = DOCA_JOB_FLAGS_NONE;
	dma_jobs[lcore_id].base.ctx = states[lcore_id].ctx;
	dma_jobs[lcore_id].dst_buff = host_dma_bufs[lcore_id];
	dma_jobs[lcore_id].src_buff = dpu_dma_bufs[lcore_id];

	/* Set data position in src_buff */
	result = doca_buf_set_data(dpu_dma_bufs[lcore_id], dpu_dma_send_bufs[lcore_id], dpu_dma_send_buf_len);
	if (result != DOCA_SUCCESS) {
		DOCA_LOG_ERR("Failed to set data for DOCA buffer: %s", doca_get_error_string(result));
		return result;
	}

	DOCA_LOG_INFO("lcore %2zu: DOCA init successful", ::mica::util::lcore.lcore_id());

	//dpu_dma_send_buf[0] = 9;
	/*uint8_t a = 17;
	memcpy(dpu_dma_send_buf, &a, sizeof(uint8_t));
	result = doca_workq_submit(state.workq, &dma_job.base);
        while ((result = doca_workq_progress_retrieve(state.workq, &event, DOCA_WORKQ_RETRIEVE_FLAGS_NONE)) ==
               DOCA_ERROR_AGAIN) {
        }
        if (result != DOCA_SUCCESS) {
                DOCA_LOG_ERR("Failed to retrieve DMA job: %s", doca_get_error_string(result));
                return result;
        }*/

        /* event result is valid */
        /*result = (doca_error_t)event.result.u64;
        if (result != DOCA_SUCCESS) {
                DOCA_LOG_ERR("DMA job event returned unsuccessfully: %s", doca_get_error_string(result));
                return result;
        }
	printf("Memory content: %" PRIu8 "\n", dpu_dma_send_buf[0]);
	while(1) {}*/

	return result;
}

template <class StaticConfig>
doca_error_t DatagramServer<StaticConfig>::RequestAccessor::doca_dma_write(uint16_t lcore_id) {
	doca_error_t res;
	//size_t lcore_id = ::mica::util::lcore.lcore_id();
	//printf("About to submit DMA job\n");
       
	res = doca_workq_progress_retrieve(states[lcore_id].workq, &events[lcore_id], DOCA_WORKQ_RETRIEVE_FLAGS_NONE);
	if (res == DOCA_ERROR_IO_FAILED || res == DOCA_ERROR_INVALID_VALUE) {
		 DOCA_LOG_ERR("lcore %" PRIu16 ": Failed to progress DMA job: %s", lcore_id, doca_get_error_string(res));
	}
	
	/* Enqueue DMA job */
        res = doca_workq_submit(states[lcore_id].workq, &dma_jobs[lcore_id].base);
        if (res != DOCA_SUCCESS) {
                DOCA_LOG_ERR("Failed to submit DMA job: %s", doca_get_error_string(res));
                doca_buf_refcount_rm(dpu_dma_bufs[lcore_id], NULL);
                doca_buf_refcount_rm(host_dma_bufs[lcore_id], NULL);
                doca_mmap_destroy(remote_mmaps[lcore_id]);
                free(dpu_dma_send_bufs[lcore_id]);
                dma_cleanup(&states[lcore_id], dma_ctxs[lcore_id]);
                return res;
        }
	//printf("Submitted DMA job\n");
	//num_dma_writes[lcore_id] = 21;

        /* Wait for job completion */
        /*while ((res = doca_workq_progress_retrieve(states[lcore_id].workq, &events[lcore_id], DOCA_WORKQ_RETRIEVE_FLAGS_NONE)) ==
               DOCA_ERROR_AGAIN) {
                //ts.tv_nsec = SLEEP_IN_NANOS;
                //nanosleep(&ts, &ts);
        }*/

//	printf("lcore %" PRIu16 ": DMAed %" PRIu8 " to host\n", lcore_id, dpu_dma_send_bufs[lcore_id][0]);	

        /*if (res != DOCA_SUCCESS) {
                DOCA_LOG_ERR("Failed to retrieve DMA job: %s", doca_get_error_string(res));
                return res;
        }*/

        /* event result is valid */
        /*res = (doca_error_t)events[lcore_id].result.u64;
        if (res != DOCA_SUCCESS) {
                DOCA_LOG_ERR("DMA job event returned unsuccessfully: %s", doca_get_error_string(res));
                return res;
        }*/
	return res;
}

template <class StaticConfig>
doca_error_t DatagramServer<StaticConfig>::save_config_info_to_files(char *export_desc, size_t export_desc_len, const char *src_buffer, size_t src_buffer_len,
			  char *export_desc_file_path, char *buffer_info_file_path)
{
	FILE *fp;
	uint64_t buffer_addr = (uintptr_t)src_buffer;
	uint64_t buffer_len = (uint64_t)src_buffer_len;

	fp = fopen(export_desc_file_path, "wb");
	if (fp == NULL) {
		DOCA_LOG_ERR("Failed to create the DMA copy file");
		return DOCA_ERROR_IO_FAILED;
	}

	if (fwrite(export_desc, 1, export_desc_len, fp) != export_desc_len) {
		DOCA_LOG_ERR("Failed to write all data into the file");
		fclose(fp);
		return DOCA_ERROR_IO_FAILED;
	}

	fclose(fp);

	fp = fopen(buffer_info_file_path, "w");
	if (fp == NULL) {
		DOCA_LOG_ERR("Failed to create the DMA copy file");
		return DOCA_ERROR_IO_FAILED;
	}

	fprintf(fp, "%" PRIu64 "\n", buffer_addr);
	fprintf(fp, "%" PRIu64 "", buffer_len);

	fclose(fp);

	return DOCA_SUCCESS;
}

template <class StaticConfig>
doca_error_t DatagramServer<StaticConfig>::save_config_info_to_buffers(const char *export_desc_file_path, const char *buffer_info_file_path, char *export_desc,
			    size_t *export_desc_len, char **remote_addr, size_t *remote_addr_len) {
	FILE *fp;
	long file_size;
	char buffer[RECV_BUF_SIZE];

	fp = fopen(export_desc_file_path, "r");
	if (fp == NULL) {
		DOCA_LOG_ERR("Failed to open %s", export_desc_file_path);
		return DOCA_ERROR_IO_FAILED;
	}

	if (fseek(fp, 0, SEEK_END) != 0) {
		DOCA_LOG_ERR("Failed to calculate file size");
		fclose(fp);
		return DOCA_ERROR_IO_FAILED;
	}

	file_size = ftell(fp);
	if (file_size == -1) {
		DOCA_LOG_ERR("Failed to calculate file size");
		fclose(fp);
		return DOCA_ERROR_IO_FAILED;
	}

	if (file_size > MAX_DMA_BUF_SIZE)
		file_size = MAX_DMA_BUF_SIZE;

	*export_desc_len = file_size;

	if (fseek(fp, 0L, SEEK_SET) != 0) {
		DOCA_LOG_ERR("Failed to calculate file size");
		fclose(fp);
		return DOCA_ERROR_IO_FAILED;
	}

	if (fread(export_desc, 1, file_size, fp) != file_size) {
		DOCA_LOG_ERR("Failed to allocate memory for source buffer");
		fclose(fp);
		return DOCA_ERROR_IO_FAILED;
	}

	fclose(fp);

	/* Read source buffer information from file */
	fp = fopen(buffer_info_file_path, "r");
	if (fp == NULL) {
		DOCA_LOG_ERR("Failed to open %s", buffer_info_file_path);
		return DOCA_ERROR_IO_FAILED;
	}

	/* Get source buffer address */
	if (fgets(buffer, RECV_BUF_SIZE, fp) == NULL) {
		DOCA_LOG_ERR("Failed to read the source (host) buffer address");
		fclose(fp);
		return DOCA_ERROR_IO_FAILED;
	}
	*remote_addr = (char *)strtoull(buffer, NULL, 0);

	memset(buffer, 0, RECV_BUF_SIZE);

	/* Get source buffer length */
	if (fgets(buffer, RECV_BUF_SIZE, fp) == NULL) {
		DOCA_LOG_ERR("Failed to read the source (host) buffer length");
		fclose(fp);
		return DOCA_ERROR_IO_FAILED;
	}
	*remote_addr_len = strtoull(buffer, NULL, 0);

	fclose(fp);

	return DOCA_SUCCESS;
}

template <class StaticConfig>
DatagramServer<StaticConfig>::~DatagramServer() {}

template <class StaticConfig>
void DatagramServer<StaticConfig>::generate_server_info() {
  std::ostringstream oss;
  oss << '{';

  oss << "\"concurrent_read\":";
  oss << (processor_->get_concurrent_read() ? "true" : "false");

  oss << ", \"concurrent_write\":";
  oss << (processor_->get_concurrent_write() ? "true" : "false");

  oss << ", \"partitions\":[";
  size_t table_count = processor_->get_table_count();
  for (size_t i = 0; i < table_count; i++) {
    if (i != 0) oss << ',';
    uint16_t lcore_id = processor_->get_owner_lcore_id(i);
    oss << '[' << i << ',' << lcore_id << ']';
  }
  oss << ']';

  oss << ", \"endpoints\":[";
  auto eids = network_->get_endpoints();
  for (size_t i = 0; i < eids.size(); i++) {
    auto& ei = network_->get_endpoint_info(eids[i]);
    if (i != 0) oss << ',';
    oss << '[' << eids[i] << ',' << ei.owner_lcore_id << ",\""
        << ::mica::network::NetworkAddress::str_mac_addr(ei.mac_addr) << "\",\""
        << ::mica::network::NetworkAddress::str_ipv4_addr(ei.ipv4_addr) << "\","
        << ei.udp_port << ']';
  }
  oss << ']';

  oss << '}';

  server_info_ = oss.str();

  if (StaticConfig::kVerbose) printf("server_info: %s\n", server_info_.c_str());
}

template <class StaticConfig>
void DatagramServer<StaticConfig>::directory_proc_wrapper(void* arg) {
  auto server = reinterpret_cast<DatagramServer<StaticConfig>*>(arg);
  server->directory_proc();
}

template <class StaticConfig>
void DatagramServer<StaticConfig>::directory_proc() {
  ::mica::util::lcore.pin_thread(directory_refresh_lcore_);

  if (directory_refresh_interval_ == 0) return;

  uint64_t last_rebalance = stopwatch_.now();

  while (!stopping_) {
    uint64_t now = stopwatch_.now();
    if (rebalance_interval_ != 0 &&
        stopwatch_.diff_in_cycles(now, last_rebalance) >=
            rebalance_interval_ * stopwatch_.c_1_sec()) {
      printf("rebalancing\n");
      last_rebalance = now;

      processor_->rebalance_load();
      processor_->wait_for_pending_owner_lcore_changes();

      generate_server_info();
      dir_client_->register_server(server_info_.c_str());
    } else
      dir_client_->refresh_server();

    for (uint32_t i = 0; i < directory_refresh_interval_ && !stopping_; i++) {
      sleep(1);
    }
  }
}

template <class StaticConfig>
void DatagramServer<StaticConfig>::run() {
  // TODO: Implement a non-DPDK option.
  uint16_t lcore_count =
      static_cast<uint16_t>(::mica::util::lcore.lcore_count());

  //std::signal(SIGINT, sig_handler);

  // Register at the directory and keep it updated.
  stopping_ = false;
  dir_client_->register_server(server_info_.c_str());
  directory_thread_ = std::thread(directory_proc_wrapper, this);

  reset_status();

  // Launch workers.
  std::vector<std::pair<DatagramServer<StaticConfig>*, uint16_t>> args(
      lcore_count);
  for (uint16_t lcore_id = 0; lcore_id < lcore_count; lcore_id++) {
    args[lcore_id].first = this;
    args[lcore_id].second = lcore_id;
  }

  for (uint16_t lcore_id = 1; lcore_id < lcore_count; lcore_id++) {
    if (!rte_lcore_is_enabled(static_cast<uint8_t>(lcore_id))) continue;
    rte_eal_remote_launch(worker_proc_wrapper, &args[lcore_id], lcore_id);
  }
  worker_proc_wrapper(&args[0]);

  rte_eal_mp_wait_lcore();

  // Clean up.
  stopping_ = true;
  directory_thread_.join();
  dir_client_->unregister_server();
}

template <class StaticConfig>
int DatagramServer<StaticConfig>::worker_proc_wrapper(void* arg) {
  auto v =
      reinterpret_cast<std::pair<DatagramServer<StaticConfig>*, uint16_t>*>(
          arg);
  v->first->worker_proc(v->second);
  return 0;
}

uint64_t cc = 0;

template <class StaticConfig>
void DatagramServer<StaticConfig>::RequestAccessor::parse_requests_from_dpdk(uint16_t lcore_id, uint8_t pkt_count) {
    //memcpy(dpu_dma_send_buf, &pkt_count, sizeof(uint8_t));
    dpu_dma_send_bufs[lcore_id][0] = pkt_count;
    //printf("$$$$$ Pkt_count in dpu_dma_send_bufs[%d] = %" PRIu8 "\n", lcore_id, (uint8_t)(dpu_dma_send_bufs[lcore_id][0]));

    for (uint8_t i = 0; i < pkt_count; i++) {
	// Prefetching logic similar to parse_request_batch()
	if (i+1 != pkt_count) {
	    auto next_buf =
		    (*bufs_)[static_cast<size_t>(i + 1)];
	    __builtin_prefetch(next_buf->get_data(), 0, 0);
	    __builtin_prefetch(next_buf->get_data() + 64, 0, 0);
	    //auto next_buf_1 = (MiniParsedReq*)(dpu_dma_send_buf+1 + 32*(i+1));
	    //__builtin_prefetch(next_buf_1, 1, 0);
	}
	if ((i+1)%2 == 1 && ((i+1) != pkt_count)) {
	    auto next_buf1 = (MiniParsedReq*)(dpu_dma_send_bufs[lcore_id]+1 + 32*(i+1));
	    __builtin_prefetch(next_buf1, 1, 0);
	}
	
	auto buf = (*bufs_)[i];
	RequestBatchReader<PacketBuffer> r(buf);
	if (!r.is_valid() || !r.is_request()) {
	  //if (StaticConfig::kVerbose)
	    printf("lcore %2zu: prepare_requests: invalid packet\n",
	       ::mica::util::lcore.lcore_id());
	  continue;
	}

      	auto org_next_index_to_prepare = next_index_to_prepare_;


      while (r.find_next()) {
        auto& pr = requests_[i];
	MiniParsedReq *mpr = (MiniParsedReq*)(dpu_dma_send_bufs[lcore_id]+1 + 32*i);

        pr.src_buf = buf;
        //pr.operation = static_cast<uint8_t>(r.get_operation());
        //pr.key_hash = r.get_key_hash();
        //pr.key = *(r.get_key());
	//pr.key = r.get_key();
	//printf("lcore %d: key = %" PRIu64 "\n", lcore_id, pr.key);
	//printf("lcore %d: key_hash_ = %" PRIu64 "\n", lcore_id, pr.key_hash);
        //pr.key_length = static_cast<uint8_t>(r.get_key_length());
	//printf("lcore %d: key_length = %" PRIu8 "\n", lcore_id, pr.key_length);
        //pr.value = *(r.get_value());
	//pr.value = r.get_value();
        //pr.value_length = static_cast<uint32_t>(r.get_value_length());
        pr.opaque = r.get_opaque();
	pr.timestamp = r.get_timestamp();
	//pr.last_in_packet = 1;
	/*if (i  == pkt_count - 1) {
	  pr.last_in_packet = 1;
	} else {
          pr.last_in_packet = 0;
	}*/
	mpr->key = *(r.get_key());
	mpr->value = *(r.get_value());
	//mpr->key_hash = pr.key_hash;
	mpr->key_hash = r.get_key_hash();
	//mpr->value_length = pr.value_length;
	mpr->value_length = static_cast<uint32_t>(r.get_value_length());
	//mpr->key_length = pr.key_length;
	mpr->key_length = static_cast<uint8_t>(r.get_key_length());
	//mpr->operation = pr.operation;
	mpr->operation = static_cast<uint8_t>(r.get_operation());

	/*if (mpr->operation == 4) {
		printf("Key_hash = %" PRIu64 "\n", mpr->key_hash);
		printf("Value = %" PRIu64 "\n", mpr->value);
	}*/

	//memcpy(dpu_dma_send_buf+1 + 56*i, &pr, 56);
	//ParsedRequest *pr_1 = (ParsedRequest*)(dpu_buffer+1 + 56*i);
	//printf("Key = %" PRIu64 "\n", mpr->key);
	//printf("Key_hash = %" PRIu64 "\n", mpr->key_hash);
	//printf("Last_in_packet = %" PRIu8 "\n", pr_1->last_in_packet);
      }

      /*if (org_next_index_to_prepare == i) {
      	 // An (effectively) empty request batch; release immediately.
         server_->network_->release(buf);
         continue;
      }*/

      /*auto last_prepared =
            (i + kParsedRequestMask) & kParsedRequestMask;
      requests_[last_prepared].last_in_packet = 1;

      memcpy(dpu_dma_send_buf+1 + 56*i, &requests_[i], 56);*/
    }

    // Time taken to parse request batch
    /*if (enable_printf) {
            parse_end_time = ra_stopwatch_.now();
	    tot_parse_time += ra_stopwatch_.diff_in_us(parse_end_time, parse_start_time);
            dma_start_time = parse_end_time;
    }*/

    doca_error_t result = doca_dma_write(lcore_id);

    // Time to write request batch to dpu
    /*if (enable_printf) {
	    dma_end_time = ra_stopwatch_.now();
	    tot_dma_time += ra_stopwatch_.diff_in_us(dma_end_time, dma_start_time);
	    wait_start_time = dma_end_time;
	    response_start_time1 = dma_end_time;
    }*/
    
    //std::this_thread::sleep_for(std::chrono::milliseconds(200));
    /*struct timespec ts;
    ts.tv_sec = 0;
    ts.tv_nsec = 1;
    uint8_t a = 0;
        while(1) {
                // Wait until host sends response
                while (((uint8_t)dpu_dma_recv_buf[0]) == 0) {
                        //DOCA_LOG_INFO("Memory content: %s", host_dma_buf);
                        //sleep(10);
                        nanosleep(&ts, &ts);
                }
                printf("Response received from host: %" PRIu8 "\n", (uint8_t)dpu_dma_recv_buf[0]);
		//print_response_in_dpu_dma_recv_buf((uint8_t)dpu_dma_recv_buf[0]);
                memcpy(dpu_dma_recv_buf, &a, sizeof(uint8_t));
		break;
        }
    */
    
    prepare_response_batch(lcore_id, pkt_count);
    
    /*if (enable_printf) {
	    response_end_time1 = ra_stopwatch_.now();
	    tot_response_time1 += ra_stopwatch_.diff_in_us(response_end_time1, response_start_time1);
    }*/

    process_response_from_host(lcore_id);
}

template <class StaticConfig>
uint8_t DatagramServer<StaticConfig>::check_dma_recv_buf_for_response(uint16_t lcore_id) {
    uint8_t count_t = 0;
    while(count_t == 0) {
            count_t = (uint8_t)(dpu_dma_recv_bufs[lcore_id][0]);
    }
    dpu_dma_recv_bufs[lcore_id][0] = 0;
    return count_t;
}

std::vector<uint8_t> responses1(8, 0);

template <class StaticConfig>
void DatagramServer<StaticConfig>::RequestAccessor::dummy_parse(uint16_t lcore_id, uint16_t pkt_count) {
    for (uint16_t i = 0; i < pkt_count; i++) {
        auto buf = (*bufs_)[i];
        RequestBatchReader<PacketBuffer> r(buf);
        if (!r.is_valid() || !r.is_request()) {
          //if (StaticConfig::kVerbose)
            printf("lcore %2zu: prepare_requests: invalid packet\n",
               ::mica::util::lcore.lcore_id());
          continue;
        }

        auto org_next_index_to_prepare = next_index_to_prepare_;

	while (r.find_next()) {
		auto& pr = requests_[i];
		pr.src_buf = buf;
	        pr.operation = static_cast<uint8_t>(r.get_operation());
	        pr.key_hash = r.get_key_hash();
	        pr.key = r.get_key();
	        pr.key_length = static_cast<uint8_t>(r.get_key_length());
	        pr.value = r.get_value();
	        pr.value_length = static_cast<uint32_t>(r.get_value_length());
	        pr.opaque = r.get_opaque();
		pr.timestamp = r.get_timestamp();
	}
    }

    // Perform dummy dma_write to host
    dpu_dma_send_bufs[lcore_id][0] = pkt_count;
    doca_dma_write(lcore_id);

    //check_dma_recv_buf_for_response(lcore_id);
    /*uint8_t tt = 0;
    while(tt == 0) {
	//usleep(1);
	memcpy(&tt, temp_buf_, sizeof(uint8_t));
    }*/
    //temp_buf_[0] = 0;
    //usleep(5);
    /*if (dpu_dma_recv_bufs[lcore_id][0] != 0) {
	    printf("lcore %2zu: Received %" PRIu8 " responses from host\n", lcore_id, dpu_dma_recv_bufs[lcore_id][0]);
	    dpu_dma_recv_bufs[lcore_id][0] = 0;
    }*/
    // Wait for response from host
    responses1[lcore_id] = 0;
    //char* temp_buf1 = dpu_dma_recv_bufs[lcore_id];
    //printf("lcore %2zu: Waiting for response from host\n", lcore_id);
    while(responses1[lcore_id] == 0) {
	//memcpy(&responses1, dpu_dma_recv_bufs[lcore_id], sizeof(uint8_t));
	responses1[lcore_id] = dpu_dma_recv_bufs[lcore_id][0];
    }
    //printf("lcore %2zu: Received %" PRIu8 " responses from host\n", lcore_id, responses1);
    dpu_dma_recv_bufs[lcore_id][0] = 0;

    //printf("lcore %2zu: Received %" PRIu8 " responses from host\n", lcore_id, dpu_dma_recv_bufs[lcore_id][0]);

    // Send response
    for (uint16_t i = 0; i < pkt_count; i++) {
	set_result(i, Result::kNotFound);
	set_out_value_length(i, 0);
    	pending_response_batch_.b.append_request_no_key_value(
    	    get_operation(i), pending_response_batch_.result, get_opaque(i),
    	    get_key_hash(i), 0, pending_response_batch_.value_length, get_request_timestamp(i));
	flush_pending_response_batch(requests_[i & kParsedRequestMask].src_buf);
	make_new_pending_response_batch();
	server_->network_->release(requests_[i & kParsedRequestMask].src_buf);

	worker_stats_->num_operations_done++;
        if (pending_response_batch_.result == Result::kSuccess)
          worker_stats_->num_operations_succeeded++;

        next_index_to_retire_++;
    }
}

template <class StaticConfig>
void DatagramServer<StaticConfig>::RequestAccessor::prepare_response_batch(uint16_t lcore_id, uint8_t pkt_count) {
    for (uint8_t i = 0; i < pkt_count; i++) {
	// Here value length is 0 for 100% GETs.
	// TODO: Pass value length based on response from CPU
	// DONE
	// Comment out below if condition for 100% GETs since value_length will always be 0.
	/*auto op = get_operation_from_dpu_dma_send_buf(i, lcore_id);
	if (op == Operation::kGet) {
	    pending_response_batch_.b.append_request_base(op, get_opaque(i), get_key_hash_from_dpu_dma_send_buf(i, lcore_id), 0, 8, get_request_timestamp(i));
	} else {
	    pending_response_batch_.b.append_request_base(op, get_opaque(i), get_key_hash_from_dpu_dma_send_buf(i, lcore_id), 0, 0, get_request_timestamp(i));
	}*/

        pending_response_batch_.b.append_request_base(get_operation_from_dpu_dma_send_buf(i, lcore_id), get_opaque(i),
			get_key_hash_from_dpu_dma_send_buf(i, lcore_id), 0, 0, get_request_timestamp(i));
	flush_partial_pending_response_batch(requests_[i & kParsedRequestMask].src_buf);
	make_new_pending_response_batch();
    	server_->network_->release(requests_[i & kParsedRequestMask].src_buf);
    }
}

std::vector<uint16_t> responses(8, 0);

template <class StaticConfig>
void DatagramServer<StaticConfig>::RequestAccessor::process_response_from_host(uint16_t lcore_id) {
    responses[lcore_id] = 0;
    //printf("lcore %2zu: Waiting for response from host\n", lcore_id);
    while(responses[lcore_id] == 0) {
	//memcpy(&responses, dpu_dma_recv_buf, sizeof(uint8_t));
	responses[lcore_id] = (uint8_t)(dpu_dma_recv_bufs[lcore_id][0]);
    }
    //printf("lcore %2zu: Response received : %" PRIu8 "\n", lcore_id, responses[lcore_id]);
    /*if (enable_printf) {
	    wait_end_time = ra_stopwatch_.now();
	    tot_wait_time += ra_stopwatch_.diff_in_us(wait_end_time, wait_start_time);
	    response_start_time2 = wait_end_time;
    }*/

    dpu_dma_recv_bufs[lcore_id][0] = 0;

    //printf("[batch %d] Starting retire\n", in_batch);
    /*for (uint16_t i = 0; i < responses; i++) {
        //RequestHeader_1 *rh = (RequestHeader_1*)(dpu_dma_recv_buf+1 + 40*i);
	MiniResponse *rh = (MiniResponse*)(dpu_dma_recv_bufs[lcore_id]+1 + 16*i);

	if (i+1 != responses) {
	    //auto next_buf = (RequestHeader_1*)(dpu_dma_recv_buf+1 + 40*(i+1));
	    auto next_buf = (MiniResponse*)(dpu_dma_recv_bufs[lcore_id]+1 + 16*(i+1));
	    __builtin_prefetch(next_buf, 0, 0);
	}

        // Set out value length
        if (rh->result == 0 || rh->result == 5) {
            char* p = get_out_value(i);
            memcpy(p, &rh->value, 8);
            set_out_value_length(i, 8);
        } else {
            set_out_value_length(i, 0);
        }
        // Set result
        set_result(i, static_cast<Result>(rh->result));
        
        retire(i);
    }*/

    uint16_t i;
    uint16_t k = 3;
    for (i = 0; i < responses[lcore_id]; i++) {
        MiniResponse *mr = (MiniResponse*)(dpu_dma_recv_bufs[lcore_id]+1 + 16*i);

        if (i == k && (i+1 != responses[lcore_id])) {
            auto next_buf = (MiniResponse*)(dpu_dma_recv_bufs[lcore_id]+1 + 16*(i+1));
	    k += 4;
            __builtin_prefetch(next_buf, 0, 0);
        }

	PacketBuffer* pb = rx_tx_state_->pending_tx.bufs[i];
	pending_response_batch_.b.append_result(pb, mr->result);

	// add value for GET
	if (mr->value_length > 0) {
	    pending_response_batch_.b.append_value(pb, mr->value);
	}

        // Update stats.
        worker_stats_->num_operations_done++;
        if (mr->result == 0)
          worker_stats_->num_operations_succeeded++;
    
        //next_index_to_retire_++;
    }

    server_->check_pending_tx_full_2(*rx_tx_state_);

}

template <class StaticConfig>
void DatagramServer<StaticConfig>::RequestAccessor::print_response_in_dpu_dma_recv_buf(uint8_t count) {
  for (uint8_t i = 0; i < count; i++) {
	RequestHeader_1 *rh = (RequestHeader_1*)(dpu_dma_recv_bufs[::mica::util::lcore.lcore_id()]+1 + sizeof(RequestHeader_1)*i);
	printf("operation: %" PRIu8 ", result: %" PRIu8 ", key_hash %" PRIu64 "\n", rh->operation, rh->result, rh->key_hash);
  }
}

template <class StaticConfig>
void DatagramServer<StaticConfig>::worker_proc(uint16_t lcore_id) {
  ::mica::util::lcore.pin_thread(lcore_id);

  /*int result;
  result = doca_init(lcore_id);
  if (result != DOCA_SUCCESS) {
          printf("lcore %2zu: DOCA init failure\n", ::mica::util::lcore.lcore_id());
  }
  result = doca_init_recv_buf(lcore_id);
  if (result != DOCA_SUCCESS) {
          printf("lcore %2zu: DOCA recv_buf init failure\n", ::mica::util::lcore.lcore_id());
  }*/
  //num_dma_writes[lcore_id] = 0;
  //num_recvs[lcore_id] = 0;

  printf("worker running on lcore %" PRIu16 "\n", lcore_id);

  in_batch = 0;
  ra_stopwatch_ = stopwatch_;
  // Find endpoints owned by this lcore.
  std::vector<EndpointId> my_eids;
  {
    auto eids = network_->get_endpoints();
    for (size_t i = 0; i < eids.size(); i++) {
      auto& ei = network_->get_endpoint_info(eids[i]);
      if (ei.owner_lcore_id == lcore_id) my_eids.push_back(eids[i]);
    }
  }
  if (my_eids.size() == 0) {
    if (StaticConfig::kVerbose)
      printf("no endpoints found for lcore %" PRIu16 "\n", lcore_id);
    return;
  }

  // Initialize RX/TX states.
  std::vector<RXTXState> rx_tx_states(my_eids.size());
  for (size_t i = 0; i < my_eids.size(); i++) {
    rx_tx_states[i].eid = my_eids[i];
    rx_tx_states[i].pending_tx.count = 0;
  }

  uint64_t last_status_report = stopwatch_.now();
  uint32_t report_status_check = 0;
  const uint32_t report_status_check_max = 0xffff;

  RequestAccessor ra(this, &worker_stats_[lcore_id], lcore_id);
  //printf("RequestAccessor for lcore %" PRIu16 " created with addr: %p\n", lcore_id, &ra);

  size_t next_index = 0;
  while (!stopping_) {
    auto& rx_tx_state = rx_tx_states[next_index];

    // Choose the next endpoint.
    if (++next_index == rx_tx_states.size()) next_index = 0;

    // Receive requests.
    std::array<PacketBuffer*, StaticConfig::kRXBurst> bufs;
    uint16_t count =
        network_->receive(rx_tx_state.eid, bufs.data(), StaticConfig::kRXBurst);
    uint64_t now = stopwatch_.now();

    if (count > 0) {

     if (StaticConfig::kVerbose)
        printf("lcore %2zu: received %" PRIu16 " packets\n",
               ::mica::util::lcore.lcore_id(), count);

      // Process requests and possibly send responses.
      ra.setup(&rx_tx_state, &bufs, count, now);
     /*if (enable_printf) {
	      in_batch++;
	      parse_start_time = now;
      } *//* else if (enable_printf_no_offload) {
	      in_batch++;
	      no_offload_start_time = now;
      }*/
      ra.parse_requests_from_dpdk(lcore_id, (uint8_t)count);
      //ra.dummy_parse(lcore_id, count);
//      processor_->process(ra);
    }

    //check_pending_tx_min(rx_tx_state);
    check_pending_tx_timeout(rx_tx_state, now);

    // See if we need to report the status.
    if (lcore_id == 0 && ++report_status_check >= report_status_check_max) {
      report_status_check = 0;

      double time_diff = stopwatch_.diff(now, last_status_report);
      if (time_diff >= 1.) {
        last_status_report = now;
        report_status(time_diff);
      }
    }

    processor_->apply_pending_owner_lcore_changes();

    worker_stats_[lcore_id].alive = 1;
  }

  // Clean up.
  for (auto& rx_tx_state : rx_tx_states) release_pending_tx(rx_tx_state);
}

template <class StaticConfig>
void DatagramServer<StaticConfig>::check_pending_tx_full(
    RXTXState& rx_tx_state) {
  if (rx_tx_state.pending_tx.count != StaticConfig::kTXBurst) return;

  network_->send(rx_tx_state.eid, rx_tx_state.pending_tx.bufs.data(),
                 rx_tx_state.pending_tx.count);
  
  /*if (enable_printf_no_offload) {
        uint64_t end_time = stopwatch_.now();
	tot_no_offload_time += stopwatch_.diff_in_us(end_time, no_offload_start_time);
	out_batch++;
  }*/
  /*if (enable_printf) {
          response_end_time2 = stopwatch_.now();
          tot_response_time2 += stopwatch_.diff_in_us(response_end_time2, response_start_time2);
          tot_server_time += stopwatch_.diff_in_us(response_end_time2, parse_start_time);
  }*/

  if (StaticConfig::kVerbose)
    printf("lcore %2zu: check_pending_tx_full sent %" PRIu16 " packets\n",
           ::mica::util::lcore.lcore_id(), rx_tx_state.pending_tx.count);
  rx_tx_state.pending_tx.count = 0;
}

template <class StaticConfig>
void DatagramServer<StaticConfig>::check_pending_tx_full_2(
    RXTXState& rx_tx_state) {
  if (rx_tx_state.pending_tx.count == 0) return;

  network_->send(rx_tx_state.eid, rx_tx_state.pending_tx.bufs.data(),
                 rx_tx_state.pending_tx.count);

  /*if (enable_printf) {
	  response_end_time2 = stopwatch_.now();
	  tot_response_time2 += stopwatch_.diff_in_us(response_end_time2, response_start_time2);
	  tot_server_time += stopwatch_.diff_in_us(response_end_time2, parse_start_time);
  }*/

  if (StaticConfig::kVerbose)
    printf("lcore %2zu: check_pending_tx_full_2 sent %" PRIu16 " packets\n",
           ::mica::util::lcore.lcore_id(), rx_tx_state.pending_tx.count);

  rx_tx_state.pending_tx.count = 0;
}


template <class StaticConfig>
void DatagramServer<StaticConfig>::check_pending_tx_min(
    RXTXState& rx_tx_state) {
  if (rx_tx_state.pending_tx.count != StaticConfig::kTXMinBurst) return;

  network_->send(rx_tx_state.eid, rx_tx_state.pending_tx.bufs.data(),
                 rx_tx_state.pending_tx.count);

  /*if (enable_printf_no_offload) {
        uint64_t end_time = stopwatch_.now();
	tot_no_offload_time += stopwatch_.diff_in_us(end_time, no_offload_start_time);
        out_batch++;
  }*/

  if (StaticConfig::kVerbose)
    printf("lcore %2zu: check_pending_tx_min sent %" PRIu16 " packets\n",
           ::mica::util::lcore.lcore_id(), rx_tx_state.pending_tx.count);
  rx_tx_state.pending_tx.count = 0;
}

template <class StaticConfig>
void DatagramServer<StaticConfig>::check_pending_tx_timeout(
    RXTXState& rx_tx_state, uint64_t now) {
  if (rx_tx_state.pending_tx.count == 0) return;

  /*uint64_t age =
      stopwatch_.diff_in_cycles(now, rx_tx_state.pending_tx.oldest_time);
  if (age < stopwatch_.c_1_usec() * StaticConfig::kTXBurstTimeout) return;*/

  network_->send(rx_tx_state.eid, rx_tx_state.pending_tx.bufs.data(),
                 rx_tx_state.pending_tx.count);

  /*if (enable_printf_no_offload) {
        uint64_t end_time = stopwatch_.now();
	tot_no_offload_time += stopwatch_.diff_in_us(end_time, no_offload_start_time);
        out_batch++;
  }*/

  /*if (enable_printf) {
          response_end_time2 = stopwatch_.now();
          tot_response_time2 += stopwatch_.diff_in_us(response_end_time2, response_start_time2);
          tot_server_time += stopwatch_.diff_in_us(response_end_time2, parse_start_time);
  }*/

  if (StaticConfig::kVerbose)
    printf("lcore %2zu: check_pending_tx_timeout sent %" PRIu16 " packets\n",
           ::mica::util::lcore.lcore_id(), rx_tx_state.pending_tx.count);
  rx_tx_state.pending_tx.count = 0;
}

template <class StaticConfig>
void DatagramServer<StaticConfig>::release_pending_tx(RXTXState& rx_tx_state) {
  for (uint16_t i = 0; i < rx_tx_state.pending_tx.count; i++)
    network_->release(rx_tx_state.pending_tx.bufs[i]);
}

template <class StaticConfig>
void DatagramServer<StaticConfig>::reset_status() {
  worker_stats_.resize(0);
  worker_stats_.resize(::mica::util::lcore.lcore_count(), {0, 0, 0, 0, 0});

  endpoint_stats_.resize(0);
  endpoint_stats_.resize(Network::kMaxEndpointCount, {0, 0, 0, 0, 0});

  auto eids = network_->get_endpoints();
  for (size_t i = 0; i < eids.size(); i++) {
    auto eid = eids[i];
    auto& ei = network_->get_endpoint_info(eid);
    endpoint_stats_[eid].last_rx_bursts = ei.rx_bursts;
    endpoint_stats_[eid].last_rx_packets = ei.rx_packets;
    endpoint_stats_[eid].last_tx_bursts = ei.tx_bursts;
    endpoint_stats_[eid].last_tx_packets = ei.tx_packets;
    endpoint_stats_[eid].last_tx_dropped = ei.tx_dropped;
  }
}

template <class StaticConfig>
void DatagramServer<StaticConfig>::report_status(double time_diff) {

  /*if (enable_printf) {
	  printf("parse time = %.2lf", static_cast<double>(tot_parse_time)/in_batch);
	  printf(", dma write time = %.2lf", static_cast<double>(tot_dma_time)/in_batch);
	  printf(", wait time = %.2lf", static_cast<double>(tot_wait_time)/in_batch);
	  printf(", response time1 = %.2lf", static_cast<double>(tot_response_time1)/in_batch);
	  printf(", response time2 = %.2lf", static_cast<double>(tot_response_time2)/in_batch);
	  printf(", server time = %.2lf", static_cast<double>(tot_server_time)/in_batch);
	  printf("\n");
	  tot_parse_time = 0;
	  tot_dma_time = 0;
	  tot_wait_time = 0;
	  tot_response_time1 = 0;
	  tot_response_time2 = 0;
	  tot_server_time = 0;
	  in_batch = 0;
  }*/
  /*if (enable_printf_no_offload) {
	  printf("server time = %.2lf", static_cast<double>(tot_no_offload_time)/out_batch);
	  printf("\n");
	  printf("out_batch = %" PRIu64 "\n", out_batch);
	  tot_no_offload_time = 0;
	  in_batch = 0;
	  out_batch = 0;
  }*/

  uint64_t total_alive = 0;
  uint64_t total_operations_done = 0;
  uint64_t total_operations_succeeded = 0;

  uint64_t rx_bursts = 0;
  uint64_t rx_packets = 0;
  uint64_t tx_bursts = 0;
  uint64_t tx_packets = 0;
  uint64_t tx_dropped = 0;

  for (size_t lcore_id = 0; lcore_id < ::mica::util::lcore.lcore_count();
       lcore_id++) {
    {
      uint64_t& v = worker_stats_[lcore_id].alive;
      total_alive += v;
      v = 0;
    }
    {
      uint64_t v = worker_stats_[lcore_id].num_operations_done;
      uint64_t& last_v = worker_stats_[lcore_id].last_num_operations_done;
      total_operations_done += v - last_v;
      last_v = v;
    }
    {
      uint64_t v = worker_stats_[lcore_id].num_operations_succeeded;
      uint64_t& last_v = worker_stats_[lcore_id].last_num_operations_succeeded;
      total_operations_succeeded += v - last_v;
      last_v = v;
    }
  }

  auto eids = network_->get_endpoints();
  for (size_t i = 0; i < eids.size(); i++) {
    auto eid = eids[i];
    auto& ei = network_->get_endpoint_info(eid);
    {
      uint64_t v = ei.rx_bursts;
      uint64_t& last_v = endpoint_stats_[eid].last_rx_bursts;
      rx_bursts += v - last_v;
      last_v = v;
    }
    {
      uint64_t v = ei.rx_packets;
      uint64_t& last_v = endpoint_stats_[eid].last_rx_packets;
      rx_packets += v - last_v;
      last_v = v;
    }
    {
      uint64_t v = ei.tx_bursts;
      uint64_t& last_v = endpoint_stats_[eid].last_tx_bursts;
      tx_bursts += v - last_v;
      last_v = v;
    }
    {
      uint64_t v = ei.tx_packets;
      uint64_t& last_v = endpoint_stats_[eid].last_tx_packets;
      tx_packets += v - last_v;
      last_v = v;
    }
    {
      uint64_t v = ei.tx_dropped;
      uint64_t& last_v = endpoint_stats_[eid].last_tx_dropped;
      tx_dropped += v - last_v;
      last_v = v;
    }
  }

  time_diff = std::max(0.01, time_diff);

  double success_rate =
      static_cast<double>(total_operations_succeeded) /
      static_cast<double>(std::max(total_operations_done, uint64_t(1)));

  //printf("total_operations_done =  %" PRIu64 "\n", total_operations_done);
  //printf("lcore 0: num_dma_writes: %" PRIu64 ", num_recvs: %" PRIu64 ", lcore 1: num_dma_writes %" PRIu64 ", num_recvs: %" PRIu64 "\n", num_dma_writes[0], num_recvs[0], num_dma_writes[0], num_recvs[1]);
  //printf("pkt_counts[0]: %" PRIu64 ", pkt_counts[1]: %" PRIu64 "\n", pkt_counts[0], pkt_counts[1]);
  //printf("dpu_dma_send_bufs[0] addr: %p, dpu_dma_send_bufs[1] addr: %p\n", dpu_dma_send_bufs[0], dpu_dma_send_bufs[1]);
  //printf("time_diff = %f\n", time_diff);

  printf("tput=%7.3lf Mops",
         static_cast<double>(total_operations_done) / time_diff / 1000000.);
  printf(", success_rate=%6.2lf%%", success_rate * 100.);
  printf(", RX=%7.3lf Mpps (%5.2lf ppb)",
         static_cast<double>(rx_packets) / time_diff / 1000000.,
         static_cast<double>(rx_packets) /
             static_cast<double>(std::max(rx_bursts, uint64_t(1))));
  printf(", TX=%7.3lf Mpps (%5.2lf ppb)",
         static_cast<double>(tx_packets) / time_diff / 1000000.,
         static_cast<double>(tx_packets) /
             static_cast<double>(std::max(tx_bursts, uint64_t(1))));
  printf(", TX_drop=%7.3lf Mpps",
         static_cast<double>(tx_dropped) / time_diff / 1000000.);
  printf(", threads=%2" PRIu64 "/%2zu", total_alive,
         ::mica::util::lcore.lcore_count());

  printf("\n");
  //printf("total_operations_done = %" PRIu64 "\n", total_operations_done);
  if (flush_status_report_) fflush(stdout);
}

template <class StaticConfig>
DatagramServer<StaticConfig>::RequestAccessor::RequestAccessor(
    DatagramServer<StaticConfig>* server, WorkerStats* worker_stats,
    uint16_t lcore_id)
    : server_(server), worker_stats_(worker_stats), lcore_id_(lcore_id) {
  make_new_pending_response_batch();
}

template <class StaticConfig>
DatagramServer<StaticConfig>::RequestAccessor::~RequestAccessor() {
  // Ensure we have processed all input packets.
  assert(next_packet_index_to_parse_ == packet_count_);
  assert(next_index_to_prepare_ == next_index_to_retire_);

  release_pending_response_batch();
}

template <class StaticConfig>
void DatagramServer<StaticConfig>::RequestAccessor::setup(
    RXTXState* rx_tx_state,
    std::array<PacketBuffer*, StaticConfig::kRXBurst>* bufs,
    size_t packet_count, uint64_t now) {
  rx_tx_state_ = rx_tx_state;
  bufs_ = bufs;
  packet_count_ = packet_count;
  now_ = now;

  next_packet_index_to_parse_ = 0;
  next_index_to_prepare_ = 0;
  next_index_to_retire_ = 0;
}

template <class StaticConfig>
bool DatagramServer<StaticConfig>::RequestAccessor::prepare(size_t index) {
  if (StaticConfig::kVerbose)
    printf("lcore %2zu: prepare: %zu\n", ::mica::util::lcore.lcore_id(), index);
  assert(index >= next_index_to_retire_);
  assert(index <= next_index_to_prepare_);
  if (index == next_index_to_prepare_) return parse_request_batch();
  /*if (enable_printf_no_offload) {
	  if (index == next_index_to_prepare_) {
	      parse_req_batch_start_time = ra_stopwatch_.now();
	      bool t = parse_request_batch();
	      parse_req_batch_end_time = ra_stopwatch_.now();
	      tot_parse_req_batch_time += ra_stopwatch_.diff_in_us(parse_req_batch_end_time, parse_req_batch_start_time);
	      return t;
	  }
  } else {
	  if (index == next_index_to_prepare_) return parse_request_batch();
  }*/
  return true;
}

template <class StaticConfig>
typename DatagramServer<StaticConfig>::Operation
DatagramServer<StaticConfig>::RequestAccessor::get_operation(size_t index) {
  assert(index >= next_index_to_retire_);
  assert(index < next_index_to_prepare_);
  return static_cast<Operation>(
      requests_[index & kParsedRequestMask].operation);
}

template <class StaticConfig>
typename DatagramServer<StaticConfig>::Operation
DatagramServer<StaticConfig>::RequestAccessor::get_operation_from_dpu_dma_send_buf(size_t index, uint16_t lcore_id) {
  return static_cast<Operation>(
      ((MiniParsedReq*)(dpu_dma_send_bufs[lcore_id]+1 + sizeof(MiniParsedReq)*index))->operation);
} 

template <class StaticConfig>
uint64_t DatagramServer<StaticConfig>::RequestAccessor::get_key_hash(
    size_t index) {
  assert(index >= next_index_to_retire_);
  assert(index < next_index_to_prepare_);
  return requests_[index & kParsedRequestMask].key_hash;
}

template <class StaticConfig>
uint64_t DatagramServer<StaticConfig>::RequestAccessor::get_key_hash_from_dpu_dma_send_buf(
    size_t index, uint16_t lcore_id) {
  return ((MiniParsedReq*)(dpu_dma_send_bufs[lcore_id]+1 + sizeof(MiniParsedReq)*index))->key_hash;
}

template <class StaticConfig>
uint64_t DatagramServer<StaticConfig>::RequestAccessor::get_request_timestamp(
    size_t index) {
  assert(index >= next_index_to_retire_);
  assert(index < next_index_to_prepare_);
  return requests_[index & kParsedRequestMask].timestamp;
}

template <class StaticConfig>
const char* DatagramServer<StaticConfig>::RequestAccessor::get_key(
    size_t index) {
  assert(index >= next_index_to_retire_);
  assert(index < next_index_to_prepare_);
  //return (char*)(&(requests_[index & kParsedRequestMask].key));
  return requests_[index & kParsedRequestMask].key;
}

template <class StaticConfig>
size_t DatagramServer<StaticConfig>::RequestAccessor::get_key_length(
    size_t index) {
  assert(index >= next_index_to_retire_);
  assert(index < next_index_to_prepare_);
  return requests_[index & kParsedRequestMask].key_length;
}

template <class StaticConfig>
const char* DatagramServer<StaticConfig>::RequestAccessor::get_value(
    size_t index) {
  assert(index >= next_index_to_retire_);
  assert(index < next_index_to_prepare_);
  //return (char*)(&(requests_[index & kParsedRequestMask].value));
  return requests_[index & kParsedRequestMask].value;
}

template <class StaticConfig>
size_t DatagramServer<StaticConfig>::RequestAccessor::get_value_length(
    size_t index) {
  assert(index >= next_index_to_retire_);
  assert(index < next_index_to_prepare_);
  return requests_[index & kParsedRequestMask].value_length;
}

template <class StaticConfig>
char* DatagramServer<StaticConfig>::RequestAccessor::get_out_value(
    size_t index) {
  assert(index == next_index_to_retire_);
  (void)index;
  return pending_response_batch_.b.get_out_value(0);
}

template <class StaticConfig>
size_t DatagramServer<StaticConfig>::RequestAccessor::get_out_value_length(
    size_t index) {
  assert(index == next_index_to_retire_);
  (void)index;
  return pending_response_batch_.b.get_max_out_value_length(0);
}

template <class StaticConfig>
void DatagramServer<StaticConfig>::RequestAccessor::set_out_value_length(
    size_t index, size_t len) {
  assert(index == next_index_to_retire_);
  (void)index;
  pending_response_batch_.value_length = len;
}

template <class StaticConfig>
void DatagramServer<StaticConfig>::RequestAccessor::set_result(size_t index,
                                                               Result result) {
  assert(index == next_index_to_retire_);
  (void)index;
  pending_response_batch_.result = result;
}

template <class StaticConfig>
uint32_t DatagramServer<StaticConfig>::RequestAccessor::get_opaque(
    size_t index) {
  assert(index >= next_index_to_retire_);
  assert(index < next_index_to_prepare_);
  return requests_[index & kParsedRequestMask].opaque;
}

template <class StaticConfig>
void DatagramServer<StaticConfig>::RequestAccessor::retire(size_t index) {
  assert(index == next_index_to_retire_);

  if (StaticConfig::kVerbose)
    printf("lcore %2zu: retire: %zu\n", ::mica::util::lcore.lcore_id(), index);

  //printf("Timestamp %" PRIu64 "\n", get_request_timestamp(index));

  pending_response_batch_.b.append_request_no_key_value(
      get_operation(index), pending_response_batch_.result, get_opaque(index),
      get_key_hash(index), 0, pending_response_batch_.value_length, get_request_timestamp(index));

  bool last_in_packet = requests_[index & kParsedRequestMask].last_in_packet;

  bool flush_response = last_in_packet;
  if (pending_response_batch_.b.get_max_out_key_length() == 0)
    flush_response = true;

  // Flush the current pending response.
  if (flush_response) {
    flush_pending_response_batch(requests_[index & kParsedRequestMask].src_buf);
    make_new_pending_response_batch();
  }

  // Release the request that is no longer used.
  if (last_in_packet)
    server_->network_->release(requests_[index & kParsedRequestMask].src_buf);

  // Update stats.
  worker_stats_->num_operations_done++;
  if (pending_response_batch_.result == Result::kSuccess)
    worker_stats_->num_operations_succeeded++;

  next_index_to_retire_++;
}

template <class StaticConfig>
bool DatagramServer<StaticConfig>::RequestAccessor::parse_request_batch() {
  //printf("lcore %2zu: packet_count_ %d\n", ::mica::util::lcore.lcore_id(), packet_count_);
  while (next_packet_index_to_parse_ < packet_count_) {
    // Prefetch the next packet.
    if (static_cast<uint16_t>(next_packet_index_to_parse_ + 1) !=
        packet_count_) {
      auto next_buf =
          (*bufs_)[static_cast<size_t>(next_packet_index_to_parse_ + 1)];
      __builtin_prefetch(next_buf->get_data(), 0, 0);
      __builtin_prefetch(next_buf->get_data() + 64, 0, 0);
      // __builtin_prefetch(next_buf->get_data() + 128, 0, 0);
      // __builtin_prefetch(next_buf->get_data() + 192, 0, 0);
    }

    //printf("lcore %2zu: next_packet_index_to_parse_ %" PRIu16 "\n", ::mica::util::lcore.lcore_id(), next_packet_index_to_parse_);

    auto buf = (*bufs_)[next_packet_index_to_parse_];
    next_packet_index_to_parse_++;

    RequestBatchReader<PacketBuffer> r(buf);

    if (!r.is_valid() || !r.is_request()) {
      if (StaticConfig::kVerbose)
        printf("lcore %2zu: prepare_requests: invalid packet\n",
               ::mica::util::lcore.lcore_id());
      continue;
    }

    auto org_next_index_to_prepare = next_index_to_prepare_;

    while (r.find_next()) {
      assert(
          static_cast<size_t>(next_index_to_prepare_ - next_index_to_retire_) <
          kMaxParsedRequestCount);

      auto& pr = requests_[next_index_to_prepare_ & kParsedRequestMask];

      //printf("[batch %" PRIu8 "] lcore %2zu: next_index_to_prepare_ %" PRIu16 "\n", in_batch, ::mica::util::lcore.lcore_id(), next_index_to_prepare_);

      pr.src_buf = buf;
      pr.operation = static_cast<uint8_t>(r.get_operation());
      pr.key_hash = r.get_key_hash();
      //printf("key_hash = %" PRIu64 "\n", pr.key_hash);
      pr.key = r.get_key();
      //pr.key = *(r.get_key());
      //printf("Key = %" PRIu64 "\n", *pr.key);
      pr.key_length = static_cast<uint8_t>(r.get_key_length());
      pr.value = r.get_value();
      //pr.value = *(r.get_value());
      pr.value_length = static_cast<uint32_t>(r.get_value_length());
      pr.opaque = r.get_opaque();
      pr.timestamp = r.get_timestamp();
      //printf("Timestamp %" PRIu64 "\n", r.get_timestamp());
      pr.last_in_packet = 0;

      next_index_to_prepare_++;
    }

    if (org_next_index_to_prepare == next_index_to_prepare_) {
      // An (effectively) empty request batch; release immediately.
      server_->network_->release(buf);
      continue;
    }

    // Release the request batch only after all requests in it have been
    // processed.
    auto last_prepared =
        (next_index_to_prepare_ + kParsedRequestMask) & kParsedRequestMask;
    requests_[last_prepared].last_in_packet = 1;

    // Prepare only one request batch.
    return true;
  }

  return false;
}

template <class StaticConfig>
void DatagramServer<
    StaticConfig>::RequestAccessor::make_new_pending_response_batch() {
  auto buf = server_->network_->allocate();
  assert(buf != nullptr);

  __builtin_prefetch(buf->get_data(), 1, 0);
  __builtin_prefetch(buf->get_data() + 64, 1, 0);
  // __builtin_prefetch(buf->get_data() + 128, 1, 0);
  // __builtin_prefetch(buf->get_data() + 192, 1, 0);

  pending_response_batch_.b = RequestBatchBuilder<PacketBuffer>(buf);
}

template <class StaticConfig>
void DatagramServer<
    StaticConfig>::RequestAccessor::release_pending_response_batch() {
  server_->network_->release(pending_response_batch_.b.get_buffer());
}

template <class StaticConfig>
void DatagramServer<StaticConfig>::RequestAccessor::
    flush_pending_response_batch(const PacketBuffer* src_buf) {
  const RequestBatchReader<PacketBuffer> src_r(src_buf);

  //printf("Inside flush_pending_response_batch\n");

  pending_response_batch_.b.set_src_mac_addr(src_r.get_dest_mac_addr());
  pending_response_batch_.b.set_dest_mac_addr(src_r.get_src_mac_addr());

  pending_response_batch_.b.set_src_ipv4_addr(src_r.get_dest_ipv4_addr());
  pending_response_batch_.b.set_dest_ipv4_addr(src_r.get_src_ipv4_addr());

  pending_response_batch_.b.set_src_udp_port(src_r.get_dest_udp_port());
  pending_response_batch_.b.set_dest_udp_port(src_r.get_src_udp_port());

  pending_response_batch_.b.set_response();

  /*const rte_ether_addr& addr = src_r.get_dest_mac_addr();
  printf("$$$ lcore %2zu\t src_mac_addr MAC: %02"PRIx8" %02"PRIx8" %02"PRIx8" %02"PRIx8" %02"PRIx8" %02"PRIx8"\n", ::mica::util::lcore.lcore_id(),
                        addr.addr_bytes[0], addr.addr_bytes[1],
                        addr.addr_bytes[2], addr.addr_bytes[3],
                        addr.addr_bytes[4], addr.addr_bytes[5]);
  const rte_ether_addr& addr2 = src_r.get_src_mac_addr();
  printf("$$$ lcore %2zu\t dest_mac_addr MAC: %02"PRIx8" %02"PRIx8" %02"PRIx8" %02"PRIx8" %02"PRIx8" %02"PRIx8"\n", ::mica::util::lcore.lcore_id(),
                        addr2.addr_bytes[0], addr2.addr_bytes[1],
                        addr2.addr_bytes[2], addr2.addr_bytes[3],
                        addr2.addr_bytes[4], addr2.addr_bytes[5]);
  printf("$$$ lcore %2zu\t src_ipv4_addr %"PRId32"\n", ::mica::util::lcore.lcore_id(), src_r.get_dest_ipv4_addr());
  printf("$$$ lcore %2zu\t dst_ipv4_addr %"PRId32"\n", ::mica::util::lcore.lcore_id(),  src_r.get_src_ipv4_addr());
  printf("$$$ lcore %2zu\t src_udp_port %"PRId16"\n", ::mica::util::lcore.lcore_id(), src_r.get_dest_udp_port());
  printf("$$$ lcore %2zu\t dst_udp_port %"PRId16"\n", ::mica::util::lcore.lcore_id(), src_r.get_src_udp_port());*/

  pending_response_batch_.b.finalize();

  //if (rx_tx_state_->pending_tx.count == 0)
  //  rx_tx_state_->pending_tx.oldest_time = now_;

  // Moves the buffer to PendingTX.
  rx_tx_state_->pending_tx.bufs[rx_tx_state_->pending_tx.count] =
      pending_response_batch_.b.get_buffer();
  rx_tx_state_->pending_tx.count++;

  //printf("rx_tx_state_->pending_tx.count = %d\n", rx_tx_state_->pending_tx.count);

  server_->check_pending_tx_full(*rx_tx_state_);
}

template <class StaticConfig>
void DatagramServer<StaticConfig>::RequestAccessor::
    flush_partial_pending_response_batch(const PacketBuffer* src_buf) {
  const RequestBatchReader<PacketBuffer> src_r(src_buf);

  pending_response_batch_.b.set_src_mac_addr(src_r.get_dest_mac_addr());
  pending_response_batch_.b.set_dest_mac_addr(src_r.get_src_mac_addr());

  pending_response_batch_.b.set_src_ipv4_addr(src_r.get_dest_ipv4_addr());
  pending_response_batch_.b.set_dest_ipv4_addr(src_r.get_src_ipv4_addr());

  pending_response_batch_.b.set_src_udp_port(src_r.get_dest_udp_port());
  pending_response_batch_.b.set_dest_udp_port(src_r.get_src_udp_port());

  pending_response_batch_.b.set_response();

  pending_response_batch_.b.finalize();

  // Moves the buffer to PendingTX.
  rx_tx_state_->pending_tx.bufs[rx_tx_state_->pending_tx.count] =
      pending_response_batch_.b.get_buffer();
  rx_tx_state_->pending_tx.count++;

}

}
}

#endif
