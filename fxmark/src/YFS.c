/**
 * Nanobenchmark: ADD
 *   BA. PROCESS = {append files at /test/$PROCESS}
 *       - TEST: block alloc
 */	      
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <errno.h>
#define __STDC_FORMAT_MACROS
#include <inttypes.h>
#include <stdlib.h>
#include <assert.h>
#include "fxmark.h"
#include "util.h"

static void set_test_root(struct worker *worker, char *test_root)
{
	struct fx_opt *fx_opt = fx_opt_worker(worker);
	sprintf(test_root, "%s/%d", fx_opt->root, worker->id);
}

static int pre_work(struct worker *worker)
{
	struct bench *bench = worker->bench;
  	char *page = NULL;
	char test_root[PATH_MAX];
	int rc = 0;

	/* create test root */
	set_test_root(worker, test_root);
	rc = mkdir_p(test_root);
	if (rc) return rc;

	/* allocate data buffer aligned with pagesize*/
	if(posix_memalign((void **)&(worker->page), PAGE_SIZE, PAGE_SIZE))
	  goto err_out;
	page = worker->page;
	if (!page)
		goto err_out;

out:
	return rc;
err_out:
	bench->stop = 1;
	rc = errno;
	free(page);
	goto out;
}

static int main_work(struct worker *worker)
{
	char test_root[PATH_MAX];
  	char *page = worker->page;
	struct bench *bench = worker->bench;
	int rc = 0;
	uint64_t iter = 0;

	assert(page);

	set_test_root(worker, test_root);
	int j;
	for (j = 0; !bench->stop; ++j) {
		/* create and close */
		int i;
		for(i = 0; i < 128; i++){
			char file[PATH_MAX];
			int fd;
			++iter;
			snprintf(file, PATH_MAX, "%s/n_inode_alloc-%d", 
					test_root, i);
			if ((fd = open(file, O_CREAT | O_RDWR, S_IRWXU)) == -1){
				printf("open failed\n");
				goto err_out;
			}
			/*set flag with O_DIRECT if necessary*/
			if(bench->directio && (fcntl(fd, F_SETFL, O_DIRECT)==-1)){
				printf("fcntl failed\n");
				goto err_out;
			}
			/* append */
			if (write(fd, page, PAGE_SIZE) != PAGE_SIZE){
				printf("write failed\n");
				goto err_out;
			}
			close(fd);
			if (unlink(file)){
				printf("unlink failed\n");
				goto err_out;
			}
		}
	}
out:
	worker->works = (double)iter;
	return rc;
err_out:
	bench->stop = 1;
	rc = errno;
	goto out;
}

struct bench_operations u_yfs_ops = {
	.pre_work  = pre_work, 
	.main_work = main_work,
};
