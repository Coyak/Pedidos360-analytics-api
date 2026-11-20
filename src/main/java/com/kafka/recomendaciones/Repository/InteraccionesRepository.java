package com.kafka.recomendaciones.Repository;

import com.kafka.recomendaciones.Model.Interacciones;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface InteraccionesRepository extends JpaRepository<Interacciones, Long> {
    List<Interacciones> findByUserId(String userId);
    List<Interacciones> findTop100ByOrderByTimestampDesc();
}
