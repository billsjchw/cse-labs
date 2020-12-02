#include <stdio.h>
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <errno.h>
#include <stdlib.h>
#include <assert.h>

#define PAGE_SIZE 4096
#define PATH_MAX 1024

__attribute__ ((aligned(PAGE_SIZE))) static char page[PAGE_SIZE];
__attribute__ ((aligned(PAGE_SIZE))) static char tmp_page[PAGE_SIZE];

int main(int argc, char **argv) {
	char test_root[PATH_MAX];

	//int rc = 0;

	int j;
	for (j = 0; j < PAGE_SIZE; j++) {
		page[j] = j^0xcc;
	}

	snprintf(test_root, PATH_MAX, "%s/0", argv[1]);
	mkdir(test_root, S_IRWXU);

	for (j = 0; j < 10; ++j) {
		/* create and close */
		int i;
		for(i = 0; i < 128; i++){
			char file[PATH_MAX];
			int fd;
			int rc = snprintf(file, PATH_MAX, "%s/n_inode_alloc-%d", test_root, i);
			if (rc > PATH_MAX) {
				printf("snprintf failed\n");
				goto err_out;
			}
			if ((fd = open(file, O_CREAT | O_RDWR, S_IRWXU)) == -1){
				printf("open failed\n");
				goto err_out;
			}
			/*set flag with O_DIRECT if necessary*/
			if((fcntl(fd, F_SETFL, O_DIRECT)==-1)){
				printf("fcntl failed\n");
				goto err_out;
			}
			/* append */
			if (write(fd, page, PAGE_SIZE) != PAGE_SIZE){
				printf("write failed\n");
				goto err_out;
			}
			close(fd);

			if ((fd = open(file, O_RDONLY)) == -1) {
				printf("reopen failed\n");
				goto err_out;
			}
			memset(tmp_page, 0, PAGE_SIZE);
			if (read(fd, tmp_page, PAGE_SIZE) != PAGE_SIZE) {
				printf("read failed\n");
				goto err_out;
			}
			close(fd);
			if (memcmp(page, tmp_page, PAGE_SIZE)) {
				printf("memcmp failed\n");
				goto err_out;
			}

			if (unlink(file)){
				printf("unlink failed\n");
				goto err_out;
			}
		}
	}
out:
	printf("Pass\n");
	return 0;
err_out:
	printf("Failed\n");
	return 0;
}

