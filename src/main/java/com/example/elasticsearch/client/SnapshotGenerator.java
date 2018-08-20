package com.example.elasticsearch.client;

import org.elasticsearch.action.admin.cluster.repositories.put.PutRepositoryResponse;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.RepositoryMetaData;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.snapshots.SnapshotInfo;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Objects;

public class SnapshotGenerator {

    private Client client;

    SnapshotGenerator() {
        try {
            client = new PreBuiltTransportClient(Settings.EMPTY)
                    .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("host1"), 9300));
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }

    }

    public boolean isRepositoryExist(Client client, String repositoryName) {
        boolean result = false;
        try {
            List<RepositoryMetaData> repositories = client.admin().cluster().prepareGetRepositories().get().repositories();
            if (repositories.size() > 0) {
                for (RepositoryMetaData repo : repositories)
                    result = repositoryName.equals(repo.name()) ? true : false;
            }
        } catch (Exception ex) {
            System.out.println("Exception in getRepository method: " + ex.toString());
        } finally {
            return result;
        }
    }

    public PutRepositoryResponse createRepository(Client client, String repositoryName,
                                                  String path, boolean compress) {
        PutRepositoryResponse putRepositoryResponse = null;
        try {
            if (!isRepositoryExist(client, repositoryName)) {
                Settings settings = Settings.builder()
                        .put("location", path + repositoryName)
                        .put("compress", compress).build();
                putRepositoryResponse = client.admin().cluster().preparePutRepository(repositoryName)
                        .setType("s3").setSettings(settings).get();
                System.out.println("Repository was created.");
            } else
                System.out.println(repositoryName + " repository already exists");
        } catch (Exception ex) {
            System.out.println("Exception in createRepository method: " + ex.toString());
        } finally {
            return putRepositoryResponse;
        }
    }


    public CreateSnapshotResponse createSnapshot(Client client, String repositoryName,
                                                 String snapshotName) {
        try {
            CreateSnapshotResponse createSnapshotResponse = client.admin().cluster()
                    .prepareCreateSnapshot(repositoryName, snapshotName)
                    .setWaitForCompletion(true)
                    .get();
            System.out.println("Snapshot was created.");
            return createSnapshotResponse;
        } catch (Exception ex) {
            System.out.println("Exception in createSnapshot method: " + ex.toString());
        }
        return null;
    }

    public boolean isSnapshotExist(Client client, String repositoryName, String snapshotName){
        boolean result = false;
        try {
            List<SnapshotInfo>

                    snapshotInfos = client.admin().cluster().prepareGetSnapshots(repositoryName).get().getSnapshots();
            result = snapshotInfos.stream().filter(Objects::isNull)
                    .filter(snapshotInfo -> snapshotInfo.snapshotId() != null)
                    .anyMatch(snapshotInfo -> snapshotInfo.equals(snapshotInfo.snapshotId().getName()));


        } catch (Exception ex) {
            System.out.println("Exception in getSnapshot method: " + ex.toString());
        } finally {
            return result;
        }
    }

}
