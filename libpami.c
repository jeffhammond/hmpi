#include <stdio.h>
#include <stdlib.h>

#include <pami.h>


#define PRINT_SUCCESS 0

#define TEST_ASSERT(c,m) \
        do { \
        if (!(c)) { \
                    printf(m" FAILED on rank %ld\n", world_rank); \
                    fflush(stdout); \
                  } \
        else if (PRINT_SUCCESS) { \
                    printf(m" SUCCEEDED on rank %ld\n", world_rank); \
                    fflush(stdout); \
                  } \
        sleep(1); \
        assert(c); \
        } \
        while(0);


void libpami_init(void)
{
    pami_result_t result = PAMI_ERROR;
    size_t world_size;
    size_t world_rank;

    /* initialize the client */
    char * clientname = "HMPI";
    pami_client_t client;
    result = PAMI_Client_create( clientname, &client, NULL, 0 );
    TEST_ASSERT(result == PAMI_SUCCESS,"PAMI_Client_create");

    /* query properties of the client */
    pami_configuration_t config;
    size_t num_contexts = -1;

    config.name = PAMI_CLIENT_TASK_ID;
    result = PAMI_Client_query( client, &config, 1);
    TEST_ASSERT(result == PAMI_SUCCESS,"PAMI_Client_query");
    world_rank = config.value.intval;

    config.name = PAMI_CLIENT_NUM_TASKS;
    result = PAMI_Client_query( client, &config, 1);
    TEST_ASSERT(result == PAMI_SUCCESS,"PAMI_Client_query");
    world_size = config.value.intval;

    if ( world_rank == 0 ) 
    {
        printf("PAMI starting test on %ld ranks \n", world_size);
        fflush(stdout);
    }

    config.name = PAMI_CLIENT_PROCESSOR_NAME;
    result = PAMI_Client_query( client, &config, 1);
    TEST_ASSERT(result == PAMI_SUCCESS,"PAMI_Client_query");
    printf("PAMI rank %ld is processor %s \n", world_rank, config.value.chararray);
    fflush(stdout);

    /* initialize the contexts */
    pami_context_t * contexts = NULL;
    contexts = (pami_context_t *) malloc( num_contexts * sizeof(pami_context_t) ); assert(contexts!=NULL);

    result = PAMI_Context_createv( client, &config, 0, contexts, num_contexts );
    TEST_ASSERT(result == PAMI_SUCCESS,"PAMI_Context_createv");
}


void libpami_shutdown(void)
{
    /* finalize the client */
    result = PAMI_Client_destroy( &client );
    TEST_ASSERT(result == PAMI_SUCCESS,"PAMI_Client_destroy");

}

