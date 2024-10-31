#pragma once
#ifndef MICA_DATAGRAM_DATAGRAM_SERVER_IMPL_H_
#define MICA_DATAGRAM_DATAGRAM_SERVER_IMPL_H_

namespace mica {
namespace datagram {

uint8_t total_dpu_cores = 0;

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

  generate_server_info();

  stopwatch_.init_end();

  uint64_t core_count = config.get("lcores").get_uint64();
  total_dpu_cores = (uint8_t)core_count;
  //printf("core count %" PRIu64 "\n", config.get("lcores").get_uint64());

  int result;
  for(int i = 0; i < core_count; i++) {
	  result = doca_init(i);
	  if (result != DOCA_SUCCESS) {
		  printf("lcore %2zu: DOCA init failure\n", ::mica::util::lcore.lcore_id());
	  }
  }

  char ch = std::cin.get();

  /*if (ch == 'c') {
	  printf("Continuing\n");
  }*/
  for(int i = 0; i < core_count; i++) {
	  result = doca_send_buf_init(i);
	  if (result != DOCA_SUCCESS) {
	          printf("lcore %2zu: DOCA send_buf init failure\n", ::mica::util::lcore.lcore_id());
	  }
  }

}

bool enable_printf = false;
uint64_t in_batch;
uint64_t process_start_time;
uint64_t process_end_time;
uint64_t tot_process_time;
uint64_t dma_start_time = 0;
uint64_t dma_end_time;
uint64_t tot_dma_time = 0;
//std::vector<uint64_t> begin_times;
//uint16_t out_batch;
::mica::util::Stopwatch ra_stopwatch_;
fd_set rfds;
struct timespec block;


//char *host_dma_send_buf;
std::vector<char*> host_dma_send_bufs(8);
std::vector<char*> host_dma_recv_bufs(8, NULL);

uint8_t in_progress_dpu_core = 0;
uint8_t next_core_id = 0;

//struct doca_dma *dma_ctx_1;
std::vector<doca_dma*> dma_ctx_1(8);
//struct program_core_objects state_1 = {0};
std::vector<program_core_objects> state_1(8, {0});
//struct doca_dma_job_memcpy dma_job = {0};
std::vector<doca_dma_job_memcpy> dma_jobs(8, {0});
//struct doca_mmap *remote_mmap_1;
std::vector<doca_mmap*> remote_mmap_1(8);
//struct doca_buf *host_doca_buf;
std::vector<doca_buf*> host_doca_bufs(8);
//struct doca_buf *dpu_doca_buf;
std::vector<doca_buf*> dpu_doca_bufs(8);
size_t host_dma_send_buf_len;
size_t host_dma_recv_buf_len;
//struct doca_event event = {0};
std::vector<doca_event> events(8, {0});

std::vector<uint8_t> count_t(8, 0);

template <class StaticConfig>
void  DatagramServer<StaticConfig>::sig_handler(int signum) {
	printf("\nFreeing doca resources\n");
        doca_argp_destroy();
        uint16_t lcore_count =
                static_cast<uint16_t>(::mica::util::lcore.lcore_count());
	//printf("lcore_count %" PRIu16 "\n", lcore_count);
        for (uint16_t i = 0; i < lcore_count; i++) {
		if (!rte_lcore_is_enabled(static_cast<uint8_t>(i))) continue;
                doca_buf_refcount_rm(dpu_doca_bufs[i], NULL);
                doca_buf_refcount_rm(host_doca_bufs[i], NULL);
                free(host_dma_recv_bufs[i]);
		free(host_dma_send_bufs[i]);
		doca_mmap_destroy(remote_mmap_1[i]);
		dma_cleanup(&state_1[i], dma_ctx_1[i]);
        }
	exit(signum);
}

template <class StaticConfig>
doca_error_t DatagramServer<StaticConfig>::save_config_info_to_buffers(const char *export_desc_file_path, const char *buffer_info_file_path, char *export_desc,
			    size_t *export_desc_len, char **remote_addr, size_t *remote_addr_len)
{
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
doca_error_t DatagramServer<StaticConfig>::doca_send_buf_init(int lcore_id) {
	doca_error_t res_1;
	//size_t lcore_id = ::mica::util::lcore.lcore_id();

        res_1 = doca_dma_create(&dma_ctx_1[lcore_id]);
        if (res_1 != DOCA_SUCCESS) {
                DOCA_LOG_ERR("Unable to create DMA engine: %s", doca_get_error_string(res_1));
                return res_1;
        }

	state_1[lcore_id].ctx = doca_dma_as_ctx(dma_ctx_1[lcore_id]);

        struct dma_config dma_conf_1 = {0};
        struct doca_pci_bdf pcie_dev_1;
	uint32_t max_chunks = 2;
	//char *export_desc_1;i
	char export_desc_1[1024] = {0};
	char *remote_addr_1 = NULL;
	size_t export_desc_len_1 = 0, remote_addr_len_1 = 0;

	char desc_txt[17];
	snprintf(desc_txt, 17, "dpu_desc%d.txt", lcore_id);
	char buf_txt[17];
	snprintf(buf_txt, 17, "dpu_buf%d.txt", lcore_id);
	strcpy(dma_conf_1.pci_address, "01:00.0");
	strcpy(dma_conf_1.export_desc_path, desc_txt);
	strcpy(dma_conf_1.buf_info_path, buf_txt);

        res_1 = parse_pci_addr(dma_conf_1.pci_address, &pcie_dev_1);
        if (res_1 != DOCA_SUCCESS) {
                DOCA_LOG_ERR("Failed to parse pci address: %s", doca_get_error_string(res_1));
                //return EXIT_FAILURE;
                return res_1;
        }

        res_1 = open_doca_device_with_pci(&pcie_dev_1, &dma_jobs_is_supported, &state_1[lcore_id].dev);
        if (res_1 != DOCA_SUCCESS) {
                doca_dma_destroy(dma_ctx_1[lcore_id]);
                return res_1;
        }

        res_1 = init_core_objects(&state_1[lcore_id], DOCA_BUF_EXTENSION_NONE, WORKQ_DEPTH, max_chunks);
        if (res_1 != DOCA_SUCCESS) {
                dma_cleanup(&state_1[lcore_id], dma_ctx_1[lcore_id]);
                return res_1;
        }

	/* Copy all relevant information into local buffers */
	save_config_info_to_buffers(dma_conf_1.export_desc_path, dma_conf_1.buf_info_path, export_desc_1, &export_desc_len_1,
				    &remote_addr_1, &remote_addr_len_1);

        //size_t host_dma_send_buf_len = sizeof(RequestHeader_1) * StaticConfig::kTXBurst + 1;
	host_dma_send_buf_len = remote_addr_len_1;
        //printf("lcore %2zu: size of host_dma_send_buf %d\n", ::mica::util::lcore.lcore_id(), host_dma_send_buf_len);
        //host_dma_send_buf = (char*)malloc(host_dma_send_buf_len);
	int r = posix_memalign((void**)&host_dma_send_bufs[lcore_id], 64, host_dma_send_buf_len);
        //if (host_dma_send_buf == NULL) {
	if (r != 0) {
                DOCA_LOG_ERR("Host send buf allocation failed");
		dma_cleanup(&state_1[lcore_id], dma_ctx_1[lcore_id]);
        }
        memset(host_dma_send_bufs[lcore_id], 0, host_dma_send_buf_len);
	//printf("lcore %d: host_dma_send_bufs[%d] addr: %p\n", lcore_id, lcore_id, host_dma_send_bufs[lcore_id]);
	/*uint8_t aa = 12;
	memcpy(host_dma_send_buf, &aa, 8);*/

        /* Populate the memory map with the allocated host_dma_send_buf memory */
        res_1 = doca_mmap_populate(state_1[lcore_id].mmap, host_dma_send_bufs[lcore_id], host_dma_send_buf_len, PAGE_SIZE, NULL, NULL);
        if (res_1 != DOCA_SUCCESS) {
                printf("mmap_populate failure\n");
		free(host_dma_send_bufs[lcore_id]);
		dma_cleanup(&state_1[lcore_id], dma_ctx_1[lcore_id]);
                return res_1;
        }

	/* Create a local DOCA mmap from exported data */
	res_1 = doca_mmap_create_from_export(NULL, (const void *)export_desc_1, export_desc_len_1, state_1[lcore_id].dev,
					   &remote_mmap_1[lcore_id]);
	if (res_1 != DOCA_SUCCESS) {
		free(host_dma_send_bufs[lcore_id]);
		dma_cleanup(&state_1[lcore_id], dma_ctx_1[lcore_id]);
		return res_1;
	}

	/* Construct DOCA buffer for each address range */
	res_1 = doca_buf_inventory_buf_by_addr(state_1[lcore_id].buf_inv, remote_mmap_1[lcore_id], remote_addr_1, remote_addr_len_1, &dpu_doca_bufs[lcore_id]);
	if (res_1 != DOCA_SUCCESS) {
		DOCA_LOG_ERR("Unable to acquire DOCA buffer representing remote buffer: %s",
			     doca_get_error_string(res_1));
		doca_mmap_destroy(remote_mmap_1[lcore_id]);
		free(host_dma_send_bufs[lcore_id]);
		dma_cleanup(&state_1[lcore_id], dma_ctx_1[lcore_id]);
		return res_1;
	}

	/* Construct DOCA buffer for each address range */
	res_1 = doca_buf_inventory_buf_by_addr(state_1[lcore_id].buf_inv, state_1[lcore_id].mmap, host_dma_send_bufs[lcore_id], host_dma_send_buf_len, &host_doca_bufs[lcore_id]);
	if (res_1 != DOCA_SUCCESS) {
		DOCA_LOG_ERR("Unable to acquire DOCA buffer representing destination buffer: %s",
			     doca_get_error_string(res_1));
		doca_buf_refcount_rm(host_doca_bufs[lcore_id], NULL);
		doca_mmap_destroy(remote_mmap_1[lcore_id]);
		free(host_dma_send_bufs[lcore_id]);
		dma_cleanup(&state_1[lcore_id], dma_ctx_1[lcore_id]);
		return res_1;
	}

        /* Construct DMA job */
        dma_jobs[lcore_id].base.type = DOCA_DMA_JOB_MEMCPY;
        dma_jobs[lcore_id].base.flags = DOCA_JOB_FLAGS_NONE;
        dma_jobs[lcore_id].base.ctx = state_1[lcore_id].ctx;
        dma_jobs[lcore_id].dst_buff = dpu_doca_bufs[lcore_id];
        dma_jobs[lcore_id].src_buff = host_doca_bufs[lcore_id];

        /* Set data position in src_buff */
        res_1 = doca_buf_set_data(host_doca_bufs[lcore_id], host_dma_send_bufs[lcore_id], host_dma_send_buf_len);
        if (res_1 != DOCA_SUCCESS) {
                DOCA_LOG_ERR("Failed to set data for DOCA buffer: %s", doca_get_error_string(res_1));
                return res_1;
        }

	DOCA_LOG_INFO("lcore %2zu: DOCA send_buf init successful", lcore_id);

	//doca_dma_write();

	return res_1;
}

template <class StaticConfig>
doca_error_t DatagramServer<StaticConfig>::RequestAccessor::doca_dma_write(uint16_t lcore_id) {
        doca_error_t res;
	//size_t lcore_id = ::mica::util::lcore.lcore_id();
	//struct doca_event event = {0};

	//printf("lcore 0: Submitting DMA job dpu core: %" PRIu16 "\n", lcore_id);

	res = doca_workq_progress_retrieve(state_1[lcore_id].workq, &events[lcore_id], DOCA_WORKQ_RETRIEVE_FLAGS_NONE);
        if (res == DOCA_ERROR_IO_FAILED || res == DOCA_ERROR_INVALID_VALUE) {
                 DOCA_LOG_ERR("lcore %" PRIu16 ": Failed to progress DMA job: %s", lcore_id, doca_get_error_string(res));
        }

        /* Enqueue DMA job */
        res = doca_workq_submit(state_1[lcore_id].workq, &dma_jobs[lcore_id].base);
        if (res != DOCA_SUCCESS) {
                DOCA_LOG_ERR("Failed to submit DMA job: %s", doca_get_error_string(res));
                doca_buf_refcount_rm(dpu_doca_bufs[lcore_id], NULL);
                doca_buf_refcount_rm(host_doca_bufs[lcore_id], NULL);
                doca_mmap_destroy(remote_mmap_1[lcore_id]);
                free(host_dma_send_bufs[lcore_id]);
                dma_cleanup(&state_1[lcore_id], dma_ctx_1[lcore_id]);
                return res;
        }
	//printf("Submitted DMA job\n");

        /* Wait for job completion */
        /*while ((res = doca_workq_progress_retrieve(state_1[lcore_id].workq, &events[lcore_id], DOCA_WORKQ_RETRIEVE_FLAGS_NONE)) ==
               DOCA_ERROR_AGAIN) {
                //ts.tv_nsec = SLEEP_IN_NANOS;
                //nanosleep(&ts, &ts);
        }*/

	//printf("lcore %" PRIu16 ": DMAed %" PRIu8 " to dpu\n", lcore_id, host_dma_send_bufs[lcore_id][0]);
	/*host_dma_send_bufs[lcore_id][0] = 0;

        if (res != DOCA_SUCCESS) {
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

std::vector<program_core_objects> states(8, {0});

template <class StaticConfig>
doca_error_t DatagramServer<StaticConfig>::doca_init(int lcore_id) {
        struct dma_config dma_conf = {0};
        struct doca_pci_bdf pcie_dev;
        doca_error_t result;
	//struct program_core_objects state = {0};
	//size_t lcore_id = ::mica::util::lcore.lcore_id();

	char desc_txt[17];
	snprintf(desc_txt, 17, "host_desc%d.txt", lcore_id);
	char buf_txt[17];
	snprintf(buf_txt, 17, "host_buf%d.txt", lcore_id);
        strcpy(dma_conf.pci_address, "01:00.0");
        strcpy(dma_conf.export_desc_path, desc_txt);
        strcpy(dma_conf.buf_info_path, buf_txt);

        result = doca_argp_init("dma_copy_host", &dma_conf);
        if (result != DOCA_SUCCESS) {
                DOCA_LOG_ERR("Failed to init ARGP resources: %s", doca_get_error_string(result));
		return result;
        }
        result = register_dma_params();
        if (result != DOCA_SUCCESS) {
                DOCA_LOG_ERR("Failed to register DMA sample parameters: %s", doca_get_error_string(result));
		return result;
        }
	result = parse_pci_addr(dma_conf.pci_address, &pcie_dev);
	if (result != DOCA_SUCCESS) {
		DOCA_LOG_ERR("Failed to parse pci address: %s", doca_get_error_string(result));
		doca_argp_destroy();
		return result;
	}

	/* Open the relevant DOCA device */
	result = open_doca_device_with_pci(&pcie_dev, &dma_jobs_is_supported, &states[lcore_id].dev);
	if (result != DOCA_SUCCESS) {
		printf("Failed to open doca device with pcie\n");
		return result;
	}

	/* Init all DOCA core objects */
	result = host_init_core_objects(&states[lcore_id]);
	if (result != DOCA_SUCCESS) {
		printf("host_init_core_objects failed\n");
		host_destroy_core_objects(&states[lcore_id]);
		return result;
	}

        char *export_desc;
        size_t export_desc_len;

	host_dma_recv_buf_len = 32 * StaticConfig::kRXBurst + 1;
	printf("lcore %2zu: size of host_dma_recv_buf %d\n", lcore_id, host_dma_recv_buf_len);
	//host_dma_recv_bufs[lcore_id] = (char *)malloc(host_dma_recv_buf_len);
	//int r = posix_memalign((void**)&host_dma_recv_buf, 64, host_dma_recv_buf_len);
	int r = posix_memalign((void**)&host_dma_recv_bufs[lcore_id], 64, host_dma_recv_buf_len);
	//if (host_dma_recv_bufs[lcore_id] == NULL) {
	if (r != 0) {
		DOCA_LOG_ERR("Host recv buf allocation failed");
		doca_argp_destroy();
	}
	memset(host_dma_recv_bufs[lcore_id], 0, host_dma_recv_buf_len);
	//printf("lcore %2zu: host_dma_recv_bufs start address %p\n", lcore_id, host_dma_recv_bufs[lcore_id]);

	/* Populate the memory map with the allocated host_dma_recv_buf memory */
	result = doca_mmap_populate(states[lcore_id].mmap, host_dma_recv_bufs[lcore_id], host_dma_recv_buf_len, PAGE_SIZE, NULL, NULL);
	if (result != DOCA_SUCCESS) {
		DOCA_LOG_ERR("DOCA MMAP POPULATE failed: %s", doca_get_error_string(result));
		host_destroy_core_objects(&states[lcore_id]);
		return result;
	}

	result = doca_mmap_export(states[lcore_id].mmap, states[lcore_id].dev, (void **)&export_desc, &export_desc_len);
	if (result != DOCA_SUCCESS) {
		DOCA_LOG_ERR("DOCA MMAP EXPORT failed: %s", doca_get_error_string(result));
		host_destroy_core_objects(&states[lcore_id]);
		return result;
	}

	/* Saves the export desc and buffer info to files, it is the user responsibility to transfer them to the dpu */
	result = save_config_info_to_files(export_desc, export_desc_len, host_dma_recv_bufs[lcore_id], host_dma_recv_buf_len,
					   dma_conf.export_desc_path, dma_conf.buf_info_path);
	if (result != DOCA_SUCCESS) {
		DOCA_LOG_ERR("Saving config to files failed: %s", doca_get_error_string(result));
		free(export_desc);
		host_destroy_core_objects(&states[lcore_id]);
		return result;
	}

	DOCA_LOG_INFO("lcore %2zu: DOCA init successful", lcore_id);

	return result;
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
  for (size_t i = 1; i < table_count; i++) {
    if (i != 1) oss << ',';
    uint16_t lcore_id = processor_->get_owner_lcore_id(i);
    oss << '[' << i << ',' << lcore_id << ']';
    //oss << '[' << 1 << ',' << 1 << ']';
    //break;
  }
  oss << ']';

  oss << ", \"endpoints\":[";
  auto eids = network_->get_endpoints();
  for (size_t i = 1; i < eids.size(); i++) {
    auto& ei = network_->get_endpoint_info(eids[i]);
    if (i != 1) oss << ',';
    oss << '[' << eids[i] << ',' << ei.owner_lcore_id << ",\""
    //oss << '[' << 1 << ',' << 1 << ",\""
        << ::mica::network::NetworkAddress::str_mac_addr(ei.mac_addr) << "\",\""
        << ::mica::network::NetworkAddress::str_ipv4_addr(ei.ipv4_addr) << "\","
        << ei.udp_port << ']';
	//<< 1 << ']';
    //break;
  }
  oss << ']';

  oss << '}';

  server_info_ = oss.str();

  //if (StaticConfig::kVerbose)
  printf("server_info: %s\n", server_info_.c_str());
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

struct timespec ts_t = { .tv_sec = 0, .tv_nsec = 15000};
uint16_t k = 3;

template <class StaticConfig>
uint8_t DatagramServer<StaticConfig>::check_dma_recv_buf_for_requests_1() {
	uint8_t count_t = 0;
	uint32_t gap = 0;
	while (count_t == 0) {
		count_t = (uint8_t)(host_dma_recv_bufs[::mica::util::lcore.lcore_id()][0]);
		if (!count_t) {
			gap++;
			if (gap > 60000) {
				clock_nanosleep(CLOCK_MONOTONIC, 0, &ts_t, NULL);
				gap = 0;
			}
		}
	}
	host_dma_recv_bufs[::mica::util::lcore.lcore_id()][0] = 0;
	return count_t;	
}

template <class StaticConfig>
uint8_t DatagramServer<StaticConfig>::check_all_dma_recv_bufs_for_requests() {
	uint8_t count = 0;
	uint8_t core_id = next_core_id;
	do {
		if ((uint8_t)(host_dma_recv_bufs[core_id][0]) == 0) {
			core_id = (core_id + 1 ) % total_dpu_cores;
		} else {
			in_progress_dpu_core = core_id;
			count = (uint8_t)(host_dma_recv_bufs[core_id][0]);
			//printf("lcore 0 received %" PRIu8 " requests from dpu core %" PRIu8 "\n", count, core_id);
			//printf("in_progress_dpu_core: %" PRIu8 "\n", in_progress_dpu_core);
			next_core_id = (core_id + 1 ) % total_dpu_cores;
		}
	}
	while (count == 0);
	host_dma_recv_bufs[in_progress_dpu_core][0] = 0;
	return count;
}

template <class StaticConfig>
uint8_t DatagramServer<StaticConfig>::check_dma_recv_buf_for_requests(size_t lcore_id) {
    //clock_nanosleep(CLOCK_MONOTONIC, TIMER_ABSTIME, &ts_t, NULL);
    //clock_nanosleep(CLOCK_MONOTONIC, 0, &ts_t, NULL);
    //nanosleep(&ts_t, NULL);
    count_t[lcore_id] = 0;
    while(count_t[lcore_id] == 0) {
	    //::mica::util::pause();
	    //memcpy(&count_t, host_dma_recv_buf, sizeof(uint8_t));
	    //rte_delay_us(1);
	    //int retval = pselect(1, &rfds, NULL, NULL, &block, NULL);
	    count_t[lcore_id] = (uint8_t)(host_dma_recv_bufs[lcore_id][0]);
    }
    //printf("lcore %2zu: Received %" PRIu8 " requests from DPU\n", lcore_id, count_t[lcore_id]);
    //printf("waiting to receive requests\n");
    /*while (host_dma_recv_buf[0] == 0) {
	    //cacheflush(host_dma_recv_buf, 1, DCACHE);
	    clflush((void*)host_dma_recv_buf);
    }
    count_t = (uint8_t)(host_dma_recv_buf[0]);*/
    host_dma_recv_bufs[lcore_id][0] = 0;
    return count_t[lcore_id];
}

template <class StaticConfig>
void DatagramServer<StaticConfig>::worker_proc(uint16_t lcore_id) {
    if (lcore_id == 0) {
	  ::mica::util::lcore.pin_thread(lcore_id);
	
	  printf("worker running on lcore %" PRIu16 "\n", lcore_id);
	
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
	
	  in_batch = 0;
	  ra_stopwatch_ = stopwatch_;
	
	  size_t next_index = 0;
	  while (!stopping_) {
	
	    auto& rx_tx_state = rx_tx_states[next_index];
	
	    // Choose the next endpoint.
	    if (++next_index == rx_tx_states.size()) next_index = 0;
	
	    // Receive requests.
	    //std::array<PacketBuffer*, StaticConfig::kRXBurst> bufs;
	    //uint16_t count =
	     //   network_->receive(rx_tx_state.eid, bufs.data(), StaticConfig::kRXBurst);
	    //uint16_t count = check_dma_recv_buf_for_requests(lcore_id);
	    uint16_t count =check_all_dma_recv_bufs_for_requests();
	    uint64_t now = stopwatch_.now();
	
	    if (count > 0) {
	//      in_batch++;
	      if (StaticConfig::kVerbose)
	        printf("lcore %2zu: received %" PRIu16 " packets\n",
	               ::mica::util::lcore.lcore_id(), count);
	
	//      begin_times.push_back(now);
	
	      // Process requests and possibly send responses.
	      //ra.setup(&rx_tx_state, &bufs, count, now);
	      ra.setup_dma(&rx_tx_state, count, now, in_progress_dpu_core);
	      //if (enable_printf) {
		//      in_batch++;
		  //    process_start_time = now;
	      //}
	      //ra.doca_dma_write(lcore_id);
	      processor_->process(ra);
	    }
	
	    //check_pending_tx_min(rx_tx_state);
	    //check_pending_tx_timeout(rx_tx_state, now);
	
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
    } else {
	  ::mica::util::lcore.pin_thread(lcore_id);
	
	  printf("worker running on lcore %" PRIu16 "\n", lcore_id);
	
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
	    //if (StaticConfig::kVerbose)
	      printf("no endpoints found for lcore %" PRIu16 "\n", lcore_id);
	    return;
	  }
	
	  //printf("lcore %2zu: eid_size = %d\n", lcore_id, my_eids.size());

	  // Initialize RX/TX states.
	  std::vector<RXTXState> rx_tx_states(my_eids.size());
	  for (size_t i = 0; i < my_eids.size(); i++) {
	    rx_tx_states[i].eid = my_eids[i];
	    //printf("lcore %2zu: rx_tx_states[%d].eid = %d\n", lcore_id, i, my_eids[i]);
	    rx_tx_states[i].pending_tx.count = 0;
	  }
	
	  uint64_t last_status_report = stopwatch_.now();
	  uint32_t report_status_check = 0;
	  const uint32_t report_status_check_max = 0xffff;
	
	  RequestAccessor ra(this, &worker_stats_[lcore_id], lcore_id);

	  size_t next_index = 0;
	  //printf("lcore %2zu: eid = %d\n", lcore_id, rx_tx_states[next_index].eid);
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
	      processor_->process(ra);
	    }
	
	    check_pending_tx_min(rx_tx_state);
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
}

template <class StaticConfig>
void DatagramServer<StaticConfig>::check_pending_tx_full(
    RXTXState& rx_tx_state) {
  if (rx_tx_state.pending_tx.count != StaticConfig::kTXBurst) return;

  network_->send(rx_tx_state.eid, rx_tx_state.pending_tx.bufs.data(),
                 rx_tx_state.pending_tx.count);
  if (StaticConfig::kVerbose)
    printf("lcore %2zu: check_pending_tx_full sent %" PRIu16 " packets\n",
           ::mica::util::lcore.lcore_id(), rx_tx_state.pending_tx.count);
  rx_tx_state.pending_tx.count = 0;
}

template <class StaticConfig>
void DatagramServer<StaticConfig>::check_pending_tx_min(
    RXTXState& rx_tx_state) {
  if (rx_tx_state.pending_tx.count != StaticConfig::kTXMinBurst) return;

  network_->send(rx_tx_state.eid, rx_tx_state.pending_tx.bufs.data(),
                 rx_tx_state.pending_tx.count);
  if (StaticConfig::kVerbose)
    printf("lcore %2zu: check_pending_tx_min sent %" PRIu16 " packets\n",
           ::mica::util::lcore.lcore_id(), rx_tx_state.pending_tx.count);
  rx_tx_state.pending_tx.count = 0;
}

template <class StaticConfig>
void DatagramServer<StaticConfig>::check_pending_tx_timeout(
    RXTXState& rx_tx_state, uint64_t now) {
  if (rx_tx_state.pending_tx.count == 0) return;

  uint64_t age =
      stopwatch_.diff_in_cycles(now, rx_tx_state.pending_tx.oldest_time);
  if (age < stopwatch_.c_1_usec() * StaticConfig::kTXBurstTimeout) return;

  network_->send(rx_tx_state.eid, rx_tx_state.pending_tx.bufs.data(),
                 rx_tx_state.pending_tx.count);
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
	  printf("process time = %.2lf", static_cast<double>(tot_process_time)/in_batch);
	  printf(", dma time = %.2lf", static_cast<double>(tot_dma_time)/in_batch);
	  printf("\n");
	  tot_process_time = 0;
	  tot_dma_time = 0;
	  in_batch = 0;
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
  if (flush_status_report_) fflush(stdout);
}

template <class StaticConfig>
DatagramServer<StaticConfig>::RequestAccessor::RequestAccessor(
    DatagramServer<StaticConfig>* server, WorkerStats* worker_stats,
    uint16_t lcore_id)
    : server_(server), worker_stats_(worker_stats), lcore_id_(lcore_id) {
  if (lcore_id != 0) {
	    make_new_pending_response_batch();
  }
}

template <class StaticConfig>
DatagramServer<StaticConfig>::RequestAccessor::~RequestAccessor() {
  // Ensure we have processed all input packets.
  assert(next_packet_index_to_parse_ == packet_count_);
  assert(next_index_to_prepare_ == next_index_to_retire_);

  release_pending_response_batch();
}

template <class StaticConfig>
void DatagramServer<StaticConfig>::RequestAccessor::setup_dma(
    RXTXState* rx_tx_state,
    //std::array<PacketBuffer*, StaticConfig::kRXBurst>* bufs,
    size_t packet_count, uint64_t now, uint16_t lcore_id) {
  rx_tx_state_ = rx_tx_state;
  //bufs_ = bufs;
  packet_count_ = packet_count;
  now_ = now;

  next_packet_index_to_parse_ = 0;
  next_index_to_prepare_ = 0;
  next_index_to_retire_ = 0;

  host_dma_send_bufs[lcore_id][0] = (uint8_t)packet_count;
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
  if (index == next_index_to_prepare_) {
	 if (::mica::util::lcore.lcore_id() == 0) {
		 return parse_request_batch_dma();
	 } else {
		 return parse_request_batch();
	 }
  }
  return true;
}

template <class StaticConfig>
typename DatagramServer<StaticConfig>::Operation
DatagramServer<StaticConfig>::RequestAccessor::get_operation(size_t index) {
  assert(index >= next_index_to_retire_);
  assert(index < next_index_to_prepare_);
  if (::mica::util::lcore.lcore_id() == 0) {
	  return static_cast<Operation>(
	      //requests_[index & kParsedRequestMask].operation);
	      //((MiniParsedReq*)(host_dma_recv_bufs[::mica::util::lcore.lcore_id()]+1 + sizeof(MiniParsedReq)*index))->operation);
	      ((MiniParsedReq*)(host_dma_recv_bufs[in_progress_dpu_core]+1 + sizeof(MiniParsedReq)*index))->operation);
  } else {
	  return static_cast<Operation>(
	      requests_[index & kParsedRequestMask].operation);
  }
}

template <class StaticConfig>
uint64_t DatagramServer<StaticConfig>::RequestAccessor::get_key_hash(
    size_t index) {
  assert(index >= next_index_to_retire_);
  assert(index < next_index_to_prepare_);
  //return requests_[index & kParsedRequestMask].key_hash;
  //return ((MiniParsedReq*)(host_dma_recv_bufs[::mica::util::lcore.lcore_id()]+1 + sizeof(MiniParsedReq)*index))->key_hash;
  if (::mica::util::lcore.lcore_id() == 0) {
	  return ((MiniParsedReq*)(host_dma_recv_bufs[in_progress_dpu_core]+1 + sizeof(MiniParsedReq)*index))->key_hash;
  } else {
	  return requests_[index & kParsedRequestMask].key_hash;
  }
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
  //return (char*)(&requests_[index & kParsedRequestMask].key);
  //return requests_[index & kParsedRequestMask].key;
  //return (char*)(&(((MiniParsedReq*)(host_dma_recv_bufs[::mica::util::lcore.lcore_id()]+1 + sizeof(MiniParsedReq)*index))->key));
  if (::mica::util::lcore.lcore_id() == 0) {
	  return (char*)(&(((MiniParsedReq*)(host_dma_recv_bufs[in_progress_dpu_core]+1 + sizeof(MiniParsedReq)*index))->key));
  } else {
	  return requests_[index & kParsedRequestMask].key;
  }
}

template <class StaticConfig>
size_t DatagramServer<StaticConfig>::RequestAccessor::get_key_length(
    size_t index) {
  assert(index >= next_index_to_retire_);
  assert(index < next_index_to_prepare_);
  //return requests_[index & kParsedRequestMask].key_length;
  //return ((MiniParsedReq*)(host_dma_recv_bufs[::mica::util::lcore.lcore_id()]+1 + sizeof(MiniParsedReq)*index))->key_length;
  if (::mica::util::lcore.lcore_id() == 0) {
	  return ((MiniParsedReq*)(host_dma_recv_bufs[in_progress_dpu_core]+1 + sizeof(MiniParsedReq)*index))->key_length;
  } else {
	  return requests_[index & kParsedRequestMask].key_length;
  }
}

template <class StaticConfig>
const char* DatagramServer<StaticConfig>::RequestAccessor::get_value(
    size_t index) {
  assert(index >= next_index_to_retire_);
  assert(index < next_index_to_prepare_);
  //return (char*)(&requests_[index & kParsedRequestMask].value);
  //return requests_[index & kParsedRequestMask].value;
  //return (char*)(&(((MiniParsedReq*)(host_dma_recv_bufs[::mica::util::lcore.lcore_id()]+1 + sizeof(MiniParsedReq)*index))->value));
  if (::mica::util::lcore.lcore_id() == 0) {
	  return (char*)(&(((MiniParsedReq*)(host_dma_recv_bufs[in_progress_dpu_core]+1 + sizeof(MiniParsedReq)*index))->value));
  } else {
	  return requests_[index & kParsedRequestMask].value;
  }
}

template <class StaticConfig>
size_t DatagramServer<StaticConfig>::RequestAccessor::get_value_length(
    size_t index) {
  assert(index >= next_index_to_retire_);
  assert(index < next_index_to_prepare_);
  //return requests_[index & kParsedRequestMask].value_length;
  //return ((MiniParsedReq*)(host_dma_recv_bufs[::mica::util::lcore.lcore_id()]+1 + sizeof(MiniParsedReq)*index))->value_length;
  if (::mica::util::lcore.lcore_id() == 0) {
	  return ((MiniParsedReq*)(host_dma_recv_bufs[in_progress_dpu_core]+1 + sizeof(MiniParsedReq)*index))->value_length;
  } else {
	  requests_[index & kParsedRequestMask].value_length;
  }
}

template <class StaticConfig>
char* DatagramServer<StaticConfig>::RequestAccessor::get_out_value(
    size_t index) {
  assert(index == next_index_to_retire_);
  (void)index;
  //return pending_response_batch_.b.get_out_value(0);
  //return (char*)(&(((MiniResponse*)(host_dma_send_bufs[::mica::util::lcore.lcore_id()]+1 + sizeof(MiniResponse)*index))->value));
  if (::mica::util::lcore.lcore_id() == 0) {
	  return (char*)(&(((MiniResponse*)(host_dma_send_bufs[in_progress_dpu_core]+1 + sizeof(MiniResponse)*index))->value));
  } else {
	  return pending_response_batch_.b.get_out_value(0);
  }
}

template <class StaticConfig>
size_t DatagramServer<StaticConfig>::RequestAccessor::get_out_value_length(
    size_t index) {
  assert(index == next_index_to_retire_);
  (void)index;
  //return pending_response_batch_.b.get_max_out_value_length(0);
  if (::mica::util::lcore.lcore_id() == 0) {
	  return 8; // since currently we support 8B Key and 8B value
  } else {
	  return pending_response_batch_.b.get_max_out_value_length(0);
  }
}

template <class StaticConfig>
void DatagramServer<StaticConfig>::RequestAccessor::set_out_value_length(
    size_t index, size_t len) {
  assert(index == next_index_to_retire_);
  (void)index;
  //pending_response_batch_.value_length = len;
  //((MiniResponse*)(host_dma_send_bufs[::mica::util::lcore.lcore_id()]+1 + 16*index))->value_length = len;
  if (::mica::util::lcore.lcore_id() == 0) {
	  ((MiniResponse*)(host_dma_send_bufs[in_progress_dpu_core]+1 + 16*index))->value_length = len;
  } else {
	  pending_response_batch_.value_length = len;
  }
}

template <class StaticConfig>
void DatagramServer<StaticConfig>::RequestAccessor::set_result(size_t index,
                                                               Result result) {
  assert(index == next_index_to_retire_);
  (void)index;
  //pending_response_batch_.result = result;
  //((MiniResponse*)(host_dma_send_bufs[::mica::util::lcore.lcore_id()]+1 + 16*index))->result = static_cast<uint8_t>(result);
  if (::mica::util::lcore.lcore_id() == 0) {
	  ((MiniResponse*)(host_dma_send_bufs[in_progress_dpu_core]+1 + 16*index))->result = static_cast<uint8_t>(result);
  } else {
	  pending_response_batch_.result = result;
  }
}

template <class StaticConfig>
uint32_t DatagramServer<StaticConfig>::RequestAccessor::get_opaque(
    size_t index) {
  assert(index >= next_index_to_retire_);
  assert(index < next_index_to_prepare_);
  return requests_[index & kParsedRequestMask].opaque;
}

template <class StaticConfig>
void DatagramServer<StaticConfig>::RequestAccessor::print_response_in_host_dma_buf(size_t index) {
  RequestHeader_1 *rh = (RequestHeader_1*)(host_dma_send_bufs[::mica::util::lcore.lcore_id()]+1 + sizeof(RequestHeader_1)*index);
  printf("Index: %d, operation: %" PRIu8 ", result: %" PRIu8 ", key_hash %" PRIu64 "\n", index, rh->operation, rh->result, rh->key_hash);
}

template <class StaticConfig>
void DatagramServer<StaticConfig>::RequestAccessor::append_response_to_dma_buf(size_t index, Result result, size_t value_length) {
  //RequestHeader_1 *rh = (RequestHeader_1*)(host_dma_send_buf+1 + 40*index);
  //MiniResponse *mr = (MiniResponse*)(host_dma_send_bufs[::mica::util::lcore.lcore_id()]+1 + 16*index);
  MiniResponse *mr = (MiniResponse*)(host_dma_send_bufs[in_progress_dpu_core]+1 + 16*index);
  mr->result = static_cast<uint8_t>(result);
  //mr->kv_length_vec =
  //      static_cast<uint32_t>((key_length << 24) | value_length);
  mr->value_length = value_length;
}

template <class StaticConfig>
void DatagramServer<StaticConfig>::RequestAccessor::retire(size_t index) {
    if (::mica::util::lcore.lcore_id() == 0) {
	  assert(index == next_index_to_retire_);
	
	  if (StaticConfig::kVerbose)
	    printf("lcore %2zu: retire: %zu\n", ::mica::util::lcore.lcore_id(), index);
	
	  //printf("Timestamp %" PRIu64 "\n", get_request_timestamp(index));
	
	  //append_response_to_dma_buf(index, pending_response_batch_.result, pending_response_batch_.value_length);
	
	  if (index == k && (index+1 != 32)) {
	      //auto buf = (MiniResponse*)(host_dma_send_bufs[::mica::util::lcore.lcore_id()]+1 + 16*((index+1) & 32));
	      auto buf = (MiniResponse*)(host_dma_send_bufs[in_progress_dpu_core]+1 + 16*((index+1) & 32));
	      k += 4;
	      __builtin_prefetch(buf, 1, 0);
	  }
	
	  //print_response_in_host_dma_buf(index);
	
	  /*pending_response_batch_.b.append_request_no_key_value(
	      get_operation(index), pending_response_batch_.result, get_opaque(index),
	      get_key_hash(index), 0, pending_response_batch_.value_length, get_request_timestamp(index));*/
	
	  //printf("retire: last_in_packet = %" PRIu8 "\n", requests_[index & kParsedRequestMask].last_in_packet);
	  //bool last_in_packet = requests_[index & kParsedRequestMask].last_in_packet;
	
	  //printf("packet_count_ = %d\n", packet_count_);
	  bool flush_response = (index == (packet_count_ - 1));
	  /*if (pending_response_batch_.b.get_max_out_key_length() == 0)
	    flush_response = true;*/
	
	  // Flush the current pending response.
	  if (flush_response) {
	    //flush_pending_response_batch(requests_[index & kParsedRequestMask].src_buf);
	    flush_pending_response_batch_from_host_send_buf();
	    //make_new_pending_response_batch();
	  }
	
	  // Release the request that is no longer used.
	  /*if (last_in_packet)
	    server_->network_->release(requests_[index & kParsedRequestMask].src_buf);*/
	
	  // Update stats.
	  worker_stats_->num_operations_done++;
	  if (pending_response_batch_.result == Result::kSuccess)
	    worker_stats_->num_operations_succeeded++;
	
	  next_index_to_retire_++;
    } else {
	  assert(index == next_index_to_retire_);
	
	  if (StaticConfig::kVerbose)
	    printf("lcore %2zu: retire: %zu\n", ::mica::util::lcore.lcore_id(), index);
	
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
}

template <class StaticConfig>
void DatagramServer<StaticConfig>::RequestAccessor::
    flush_pending_response_batch_from_host_send_buf() {

    /*if (enable_printf) {
	    process_end_time = ra_stopwatch_.now();
	    tot_process_time += ra_stopwatch_.diff_in_us(process_end_time, process_start_time);
	    dma_start_time = process_end_time;
    }*/

    doca_error_t res_2 = doca_dma_write((uint16_t)in_progress_dpu_core);

    /*if (enable_printf) {
	    dma_end_time = ra_stopwatch_.now();
	    tot_dma_time += ra_stopwatch_.diff_in_us(dma_end_time, dma_start_time);
    }*/

    if (res_2 != DOCA_SUCCESS) {
      printf("lcore %2zu: Unable to complete DMA write to DPU\n", ::mica::util::lcore.lcore_id());
    }
    //printf("lcore %2zu: responses sent = %" PRIu8 "\n", ::mica::util::lcore.lcore_id(), host_dma_send_buf[0]);
    //printf("Time to complete batch %" PRIu16 " = %" PRIu64 "\n", in_batch, ra_stopwatch_.diff_in_us(ra_stopwatch_.now(), begin_times[in_batch]));
    //out_batch++;
}

template <class StaticConfig>
bool DatagramServer<StaticConfig>::RequestAccessor::parse_request_batch() {
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

      pr.src_buf = buf;
      pr.operation = static_cast<uint8_t>(r.get_operation());
      pr.key_hash = r.get_key_hash();
      pr.key = r.get_key();
      pr.key_length = static_cast<uint8_t>(r.get_key_length());
      pr.value = r.get_value();
      pr.value_length = static_cast<uint32_t>(r.get_value_length());
      pr.opaque = r.get_opaque();
      pr.timestamp = r.get_timestamp();
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
bool DatagramServer<StaticConfig>::RequestAccessor::parse_request_batch_dma() {
  /*int i = 0;
  while (i < packet_count_) {
    ParsedRequest *pr_1 = (ParsedRequest*)(host_dma_buf+1 + 56*i);
    printf("Key = %" PRIu64 "\n", *pr_1->key);
    printf("Key_hash = %" PRIu64 "\n", pr_1->key_hash);
    i++;
  }*/

  while (next_packet_index_to_parse_ < packet_count_) {
    // Prefetch the next packet.
    //if (static_cast<uint16_t>(next_packet_index_to_parse_ + 1) != packet_count_) {
    if ((next_packet_index_to_parse_ + 1) % 2 == 1 && static_cast<uint16_t>(next_packet_index_to_parse_ + 1) != packet_count_) {
      //auto next_buf =
      //    (*bufs_)[static_cast<size_t>(next_packet_index_to_parse_ + 1)];
//      auto next_buf = (MiniParsedReq*)(host_dma_recv_bufs[::mica::util::lcore.lcore_id()]+1 + 32*(next_packet_index_to_parse_ + 1));
	auto next_buf = (MiniParsedReq*)(host_dma_recv_bufs[in_progress_dpu_core]+1 + 32*(next_packet_index_to_parse_ + 1));
      __builtin_prefetch(next_buf, 0, 0);
      //__builtin_prefetch(next_buf->get_data() + 64, 0, 0);
      // __builtin_prefetch(next_buf->get_data() + 128, 0, 0);
      // __builtin_prefetch(next_buf->get_data() + 192, 0, 0);
    }

    //auto buf = (*bufs_)[next_packet_index_to_parse_];
    next_packet_index_to_parse_++;

    //RequestBatchReader<PacketBuffer> r(buf);

    /*if (!r.is_valid() || !r.is_request()) {
      if (StaticConfig::kVerbose)
        printf("lcore %2zu: prepare_requests: invalid packet\n",
               ::mica::util::lcore.lcore_id());
      continue;
    }*/

    auto org_next_index_to_prepare = next_index_to_prepare_;

    //while (r.find_next()) {
      assert(
          static_cast<size_t>(next_index_to_prepare_ - next_index_to_retire_) <
          kMaxParsedRequestCount);

//      auto& pr = requests_[next_index_to_prepare_ & kParsedRequestMask];

      //pr.src_buf = buf;
      //pr.operation = static_cast<uint8_t>(r.get_operation());
      /*if (jj % 2 == 0) {
	      printf("lcore %2zu : Operation %d\n", ::mica::util::lcore.lcore_id(), pr.operation);
      }
      jj++;*/
      /*pr.key_hash = r.get_key_hash();
      pr.key = r.get_key();
      pr.key_length = static_cast<uint8_t>(r.get_key_length());
      pr.value = r.get_value();
      pr.value_length = static_cast<uint32_t>(r.get_value_length());
      pr.opaque = r.get_opaque();
      pr.timestamp = r.get_timestamp();
      //printf("Timestamp %" PRIu64 "\n", r.get_timestamp());
      pr.last_in_packet = 0;*/
      //ParsedRequest_1 *pr_1 = (ParsedRequest_1*)(host_dma_recv_buf+1 + 56*next_index_to_prepare_);

/*      MiniParsedReq *mpr = (MiniParsedReq*)(host_dma_recv_bufs[::mica::util::lcore.lcore_id()]+1 + 32*next_index_to_prepare_);
      pr.operation = mpr->operation;
      pr.key_hash = mpr->key_hash;
      pr.key_length = mpr->key_length;
      pr.key = (char*)(&mpr->key);
      pr.value = (char*)(&mpr->value);
      pr.value_length = mpr->value_length;
*/

      /*if (pr.operation == 4) {
	      printf("Key_hash = %" PRIu64 "\n", pr.key_hash);
	      printf("Value = %" PRIu64 "\n", *(pr.value));
      }*/

      /*pr.src_buf = pr_1->src_buf;
      pr.operation = pr_1->operation;
      pr.key_hash = pr_1->key_hash;
      pr.key_length = pr_1->key_length;
      pr.key = pr_1->key;
      pr.value = pr_1->value;
      pr.value_length = pr_1->value_length;
      pr.opaque = pr_1->opaque;
      pr.timestamp = pr_1->timestamp;
      pr.last_in_packet = pr_1->last_in_packet;*/
      
      //printf("Key = %" PRIu64 "\n", *pr.key);
      //printf("Key_hash = %" PRIu64 "\n", pr.key_hash);
      //printf("parse_request_batch: last_in_packet = %" PRIu8 "\n", pr.last_in_packet);

      next_index_to_prepare_++;
    //}

    /*if (org_next_index_to_prepare == next_index_to_prepare_) {
      // An (effectively) empty request batch; release immediately.
      server_->network_->release(buf);
      continue;
    }*/

    // Release the request batch only after all requests in it have been
    // processed.
    /*auto last_prepared =
        (next_index_to_prepare_ + kParsedRequestMask) & kParsedRequestMask;
    requests_[last_prepared].last_in_packet = 1;
    printf("parse_request_batch: last_prepared = %" PRIu8 "\n", last_prepared);*/

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

  if (rx_tx_state_->pending_tx.count == 0)
    rx_tx_state_->pending_tx.oldest_time = now_;

  // Moves the buffer to PendingTX.
  rx_tx_state_->pending_tx.bufs[rx_tx_state_->pending_tx.count] =
      pending_response_batch_.b.get_buffer();
  rx_tx_state_->pending_tx.count++;

  server_->check_pending_tx_full(*rx_tx_state_);
}
}
}

#endif
