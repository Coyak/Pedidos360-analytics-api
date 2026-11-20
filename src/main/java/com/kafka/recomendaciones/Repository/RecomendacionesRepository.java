package com.kafka.recomendaciones.Repository;

import com.kafka.recomendaciones.Model.Recomendaciones;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

@Repository
public interface RecomendacionesRepository extends JpaRepository<Recomendaciones, Long> {
    List<Recomendaciones> findByUserIdOrderByRankPositionAsc(String userId);

    @Transactional
    void deleteByUserId(String userId);
}
