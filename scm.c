/**
 * Tony Givargis
 * Copyright (C), 2023
 * University of California, Irvine
 *
 * CS 238P - Operating Systems
 * scm.c
 */

#define _GNU_SOURCE
#define META_SIZE 24
#define SCM_SIGNATURE 0xDEEDBEED
#define VM_ADDR 0x600000000003

#include <sys/types.h>

#include <sys/stat.h>

#include <sys/mman.h>

#include <unistd.h>

#include <fcntl.h>

#include "scm.h"

/**
 * Needs:
 *   fstat()
 *   S_ISREG()
 *   open()
 *   close()
 *   sbrk()
 *   mmap()
 *   munmap()
 *   msync()
 */

/* research the above Needed API and design accordingly */

struct metadata {
  size_t size; /* Size of the data stored */
  size_t signature; /* Unique identifier for SCM format */
  size_t checksum; /* Checksum for data integrity verification */
};

struct scm {
  int fd; /* File descriptor for the opened file */
  void * mem; /* Pointer to mapped memory region */
  size_t utilized; /* Amount of utilized space in the mapped region */
  size_t capacity; /* Total size of the mapped memory */
};

/* Determines the size of the file and checks its validity */
int file_size(struct scm * scm) {
  struct stat st;
  size_t page;

  /* Get file statistics */
  if (fstat(scm -> fd, & st) == -1) {
    close(scm -> fd);
    free(scm);
    TRACE("ERROR fstat failed");
    return -1;
  }

  /* Check if it's a regular file */
  if (S_ISREG(st.st_mode)) {
    scm -> capacity = st.st_size; /* Set length to file size */
    page = page_size();
    scm -> capacity = (scm -> capacity / page) * page; /* Align length to page size */
  } else {
    TRACE("File is not regular!!");
    FREE(scm);
    return -1;
  }

  /* Return 0 if scm->capacity is non-zero, otherwise -1 */
  return (scm -> capacity) ? -1 : 0;
}

/* Calculates checksum by XOR-ing size and signature */
uint64_t calculate_checksum(const struct metadata * meta) {
  return meta -> size ^ meta -> signature;
}

/* Initializes metadata with default values and calculated checksum */
void initialize_metadata(struct scm * scm) {
  struct metadata * meta = (struct metadata * ) scm -> mem;

  meta -> size = 0; /* Initialize size to zero */
  meta -> signature = SCM_SIGNATURE; /* Set SCM signature */
  meta -> checksum = calculate_checksum(meta); /* Compute checksum */

  scm -> utilized = 0; /* Initialize scm size */
}

/* Loads metadata from memory and validates it */
int load_metadata(struct scm * scm) {
  struct metadata * meta = (struct metadata * ) scm -> mem;

  /* Check if signature matches expected SCM_SIGNATURE */
  if (meta -> signature != SCM_SIGNATURE) {
    TRACE("ERROR - Invalid SCM signature");
    return -1;
  }

  /* Validate metadata by comparing stored checksum */
  if (meta -> checksum != calculate_checksum(meta)) {
    TRACE("ERROR - Metadata checksum validation failed");
    return -1;
  }

  scm -> utilized = meta -> size; /* Set scm size from metadata */

  return 0;
}

/* Stores metadata back into memory with updated values and checksum */
void store_metadata(struct scm * scm) {
  struct metadata * meta = (struct metadata * ) scm -> mem;

  meta -> size = scm -> utilized; /* Store current scm size */

  /* Update checksum to ensure metadata integrity */
  meta -> checksum = calculate_checksum(meta);
}

/* Initializes file content with zeroes up to a specified size */
int init_zero(const char * filename, size_t size) {
  int fd = open(filename, O_WRONLY);
  char buffer[4096];
  size_t bytes_written = 0;
  if (fd == -1) {
    return -1;
  }

  memset(buffer, 0, sizeof(buffer)); /* Zero-fill the buffer */

  /* Write zeroes until the specified size is reached */
  while (bytes_written < size) {
    ssize_t result = write(fd, buffer, sizeof(buffer));
    if (result == -1) {
      close(fd);
      return -1;
    }
    bytes_written += result;
  }
  close(fd);
  return 0;
}

/* Opens or creates an SCM structure with optional truncation */
struct scm * scm_open(const char * pathname, int truncate) {
  size_t curr, vm_addr;

  /* Allocate memory for the SCM struct */
  struct scm * scm = (struct scm * ) malloc(sizeof(struct scm));
  if (!scm) {
    TRACE("ERROR - Failed memory alloc for SCM struct!!");
    return NULL;
  }

  memset(scm, 0, sizeof(struct scm));

  /* Open the file for read/write */
  scm -> fd = open(pathname, O_RDWR);
  if (scm -> fd < 0) {
    TRACE("Failed file opening!!");
    FREE(scm);
    return NULL;
  }

  file_size(scm); /* Determine file size */

  curr = (size_t) sbrk(0); /* Get current program break */
  vm_addr = (VM_ADDR / page_size()) * page_size();
  if (vm_addr < curr) {
    TRACE("Virtual memory start address is below break line");
    FREE(scm);
    return NULL;
  }

  /* If truncate flag is set, zero out file contents */
  if (truncate) {
    struct stat st;
    if (fstat(scm -> fd, & st) == -1 || init_zero(pathname, st.st_size) != 0) {
      TRACE("ERROR - Failed to initialize file content to zero");
      close(scm -> fd);
      free(scm);
      return NULL;
    }
  }

  /* Map the file into memory */
  scm -> mem = mmap((void * ) vm_addr, scm -> capacity, PROT_READ | PROT_WRITE, MAP_FIXED | MAP_SHARED, scm -> fd, 0);

  /* Check for successful memory mapping */
  if (scm -> mem == MAP_FAILED) {
    TRACE("ERROR - Failed execution mmap!!");
    FREE(scm);
    return NULL;
  }

  /* Initialize or load metadata based on the truncate flag */
  if (truncate) {
    initialize_metadata(scm);
  } else {
    load_metadata(scm);
  }

  /* Offset memory pointer to skip metadata */
  scm -> mem = (void * )((char * ) scm -> mem + META_SIZE);

  return scm;
}

/* Returns the total capacity of the SCM memory */
size_t scm_capacity(const struct scm * scm) {
  return scm -> capacity;
}

/* Returns the currently utilized memory size within SCM */
size_t scm_utilized(const struct scm * scm) {
  return scm -> utilized;
}

/* Allocates memory within SCM and duplicates a string */
char * scm_strdup(struct scm * scm,
  const char * s) {
  size_t length;
  char * new_string;

  length = strlen(s) + 1; /* Get string length */
  new_string = (char * ) scm_malloc(scm, length); /* Allocate memory in SCM */
  strcpy(new_string, s); /* Copy string to SCM memory */
  return new_string;
}

/* Returns the base memory address of SCM */
void * scm_mbase(struct scm * scm) {
  return (void * ) scm -> mem;
}

/* Allocates memory within SCM's memory region */
void * scm_malloc(struct scm * scm, size_t n) {
  void * p;
  if ((scm -> utilized + n) <= scm -> capacity) {
    p = (void *) ((char *) scm -> mem + scm -> utilized); /* Calculate address for allocation */
    scm -> utilized += n; /* Increase utilized size */
    return p;
  }
  return NULL; /* Return NULL if no space left */
}

/* Closes the SCM file, storing metadata and unmapping memory */
void scm_close(struct scm * scm) {
  scm -> mem = (void * )((char * ) scm -> mem - META_SIZE); /* Adjust memory pointer */
  store_metadata(scm); /* Save metadata */

  /* Synchronize and unmap memory if it was successfully mapped */
  if (scm -> mem != NULL && scm -> mem != MAP_FAILED) {
    if (msync(scm -> mem, scm -> capacity, MS_SYNC) == -1) {
      TRACE("msync failed in scm_close");
    }
    if (munmap(scm -> mem, scm -> capacity) == -1) {
      TRACE("munmap failed in scm_close");
    }
  }

  if (scm -> fd) {
    if (close(scm -> fd) == -1) {
      /* Close the file descriptor */
      TRACE("ERROR - File close failed");
    }
  }
  free(scm); /* Free the scm struct memory */
}